import logging
from pathlib import Path

from fastapi import APIRouter, Depends, File, HTTPException, status, UploadFile
from fastapi.responses import FileResponse

from app.auth import AuthLevel, User, validate_api_key
from app.db import (
    delete_raw_table,
    delete_stage_table,
    load_raw_data,
    stage_data,
    union_prod,
)
from app.pg import Record, row_count, select_data
from app.pg import DateFormatError, DuplicateHeaderError, PrimaryKeysMissingError
from app.sql import prod_table, stage_schema
from app.filesys import (
    build_asset_path,
    build_raw_file_path,
    get_data_files,
    get_directories,
)

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/")
async def data(verified: bool = True):
    return select_data(prod_table(verified), "prod")


@router.post("/update", status_code=status.HTTP_204_NO_CONTENT)
async def update(
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege()
    union_prod(verified)


@router.get("/assetClasses")
def get_asset_classes(verified: bool = True):
    return {
        "verification_status": "verified" if verified else "unverified",
        "asset_classes": [d.stem for d in get_directories(verified)],
    }


@router.delete("/assetClasses/{asset_class}")
def delete_asset_class(
    asset_class: str,
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege()
    try:
        asset_path = build_asset_path(asset_class, verified)
    except ValueError:
        return HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Asset class: '{asset_class}' does not exist.",
        )
    if any(asset_path.iterdir()):
        return HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Asset class: '{asset_class}' is not empty. Please delete all files before deleting asset class.",
        )
    asset_path.rmdir()
    return "Asset class successfully deleted."


@router.post("/assetClasses/{asset_class}")
def create_asset_class(
    asset_class: str,
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege()
    if asset_class.lower().strip().replace(" ", "_") != asset_class:
        return HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Asset class names must be lowercase, and have underscores instead of spaces.",
        )
    try:
        build_asset_path(asset_class, verified, create=True)
    except FileExistsError:
        return HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Asset class '{asset_class}' already exists.",
        )
    return status.HTTP_204_NO_CONTENT


@router.get("/assetClasses/{asset_class}/files")
def get_asset_class_files(
    asset_class: str,
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege()
    try:
        files = get_data_files(asset_class, verified)
    except ValueError:
        return HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Asset class '{asset_class}' not found",
        )
    return {
        "asset_class": asset_class,
        "verification_status": "verified" if verified else "unverified",
        "file_names": [f.name for f in files],
    }


@router.post("/assetClasses/{asset_class}/uploadFile")
def upload_file(
    asset_class: str,
    file: UploadFile = File(),
    verified: bool = True,
    overwrite: bool = False,
    authenticated_user: User = Depends(validate_api_key),
):
    required_perm = AuthLevel.ADMIN if (verified or overwrite) else AuthLevel.EDIT
    authenticated_user.check_privilege(required_perm)
    file_path: Path = build_raw_file_path(file.filename, asset_class, verified)
    if file_path.exists() and not overwrite:
        return {
            "message": f"Cannot upload {asset_class}/{file.filename}, file already exists. Set overwrite flag if you are sure you want to overwrite."
        }
    try:
        contents = file.file.read()
        with open(file_path, "wb") as f:
            f.write(contents)
    except Exception as e:
        logger.exception(e)
        return {"message": "There was an error uploading the file"}
    finally:
        file.file.close()
    return {"message": f"Successfully uploaded {file.filename}"}


@router.get("/assetClasses/{asset_class}/files/{file_name}")
def download_file(
    asset_class: str,
    file_name: str,
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
) -> FileResponse:
    authenticated_user.check_privilege()
    file_path = build_raw_file_path(file_name, asset_class, verified)
    return FileResponse(file_path)


@router.delete("/assetClasses/{asset_class}/files/{file_name}")
def delete_file(
    asset_class: str,
    file_name: str,
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege()
    file_path = build_raw_file_path(file_name, asset_class, verified)
    try:
        file_path.unlink()
    except Exception as e:
        logger.exception(e)
        raise
    return "File deleted"


@router.put("/assetClasses/{asset_class}/files/{file_name}/updateRecord")
def update_record(
    record: Record,
    asset_class: str,
    file_name: str,
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege()
    project_id = record.project_id
    sample = record.sample
    data = record.data
    data.update({"sample": sample, "project_id": project_id})
    file_path = build_raw_file_path(file_name, asset_class, verified)
    p_id_idx: int | None = None
    samp_idx: int | None = None
    with open(file_path, "r", encoding="utf-8-sig") as f:
        headers = [h.lower() for h in f.readline().split(",")]
        print(headers)
        for i, h in enumerate(headers):
            if h == "project_id":
                p_id_idx = i
            if h == "sample":
                samp_idx = i
        if any([p_id_idx is None, samp_idx is None]):
            print("Couldn't find project_id or sample columns.")
            return "couldn't find ID columns"
        if not all(k.lower() in headers for k in data.keys()):
            print("Extra headers specified")
            return "Extra headers specified"
        temp_file = file_path.parent / Path(f"{file_path.stem}.tmp")
        not_yet_replaced: bool = True
        with open(temp_file, "w") as t:
            t.write(",".join(headers))
            for l in f.readlines():
                row = l.split(",")
                if (row[p_id_idx] == project_id) and (row[samp_idx] == sample):
                    t.write(
                        ",".join([data.get(h, row[i]) for i, h in enumerate(headers)])
                    )
                    not_yet_replaced = False
                else:
                    t.write(l)
            if not_yet_replaced:
                t.write(",".join([data.get(h, "") for h in headers]))
    try:
        table = load_raw_data(temp_file)
        temp_file.replace(file_path)
    except Exception as e:
        logger.exception(e)
        raise e
    return {
        "table_name": table,
        "rows": row_count(table)[0],
    }


@router.post("/assetClasses/{asset_class}/files/{file_name}/load")
def update_raw(
    asset_class: str,
    file_name: str,
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege()
    try:
        file_path = build_raw_file_path(file_name, asset_class, verified)
    except ValueError:
        return HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Asset class '{asset_class}' does not exist",
        )
    logger.info(f"Loading raw file: {file_path} into database.")
    try:
        table = load_raw_data(file_path)
    except FileNotFoundError:
        return HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"File '{file_name}' in asset class '{asset_class}' does not exist",
        )
    except DuplicateHeaderError:
        return HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Duplicate headers found. Please fix and re-upload the file.",
        )
    except PrimaryKeysMissingError:
        return HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Required primary keys missing. Make sure file has 'project_id' and 'sample' fields.",
        )
    except DateFormatError:
        return HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Date field was uploaded with improper format. Double check that your date fields are 'YYYY-MM-DD'",
        )
    except Exception as e:
        logger.exception(e)
        return HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=e
        )
    return {
        "table_name": table,
        "rows": row_count(table)[0],
    }


@router.delete("/rawTables/{table_name}")
def delete_raw(
    table_name: str,
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege()
    delete_raw_table(table_name, verified)


@router.post("/assetClasses/{asset_class}/stage/update")
def update_stage(
    asset_class: str,
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege()
    stage_data(asset_class, verified)


@router.delete("/assetClasses/{asset_class}/stage")
def delete_stage(
    asset_class: str,
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege()
    delete_stage_table(asset_class, verified)
    return status.HTTP_204_NO_CONTENT

@router.get("/assetClasses/{asset_class}/stage/data")
def get_stage_data(
    asset_class: str,
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege()
    return select_data(asset_class, schema=stage_schema(verified))


@router.post("/reference/exchangeRates/update")
def update_exchange_rate():
    pass
