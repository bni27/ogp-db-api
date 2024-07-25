import logging
from pathlib import Path

from fastapi import APIRouter, Depends, File, status, UploadFile

from app.auth import AuthLevel, User, validate_api_key
from app.db import load_raw_data, stage_data, union_prod
from app.pg import row_count, select_data
from app.sql import raw_schema, stage_schema
from app.filesys import build_raw_file_path, get_data_files, get_directories

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/")
async def data(asset_class: str | None = None):
    return select_data(asset_class, "raw_verified")


@router.post("/update", status_code=status.HTTP_204_NO_CONTENT)
async def update(
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege()
    union_prod()
    return


@router.get("/assetClasses")
def get_asset_classes(verified: bool = True):
    return {
        "verification_status": "verified" if verified else "unverified",
        "asset_classes": [d.stem for d in get_directories(verified)],
    }


@router.get("/assetClasses/{asset_class}/files")
def get_asset_class_files(
    asset_class: str,
    verified: bool = False,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege()
    files = get_data_files(asset_class, verified)
    return {
        "asset_class": asset_class,
        "verification_status": "verified" if verified else "unverified",
        "file_names": [f.name for f in files],
    }


@router.post("/assetClasses/{asset_class}/uploadFile")
def upload_file(
    asset_class: str,
    file: UploadFile = File(),
    verified: bool = False,
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
    except Exception:
        return {"message": "There was an error uploading the file"}
    finally:
        file.file.close()
    return {"message": f"Successfully uploaded {file.filename}"}


@router.post("/assetClasses/{asset_class}/files/{file_name}/load")
def update_raw(
    asset_class: str,
    file_name: str,
    verified: bool = False,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege()
    file_path = build_raw_file_path(file_name, asset_class, verified)
    try:
        table = load_raw_data(file_path)
    except Exception as e:
        logger.exception(e)
        raise e
    return {
        "table_name": table,
        "rows": row_count(table)[0],
    }


@router.post("/assetClasses/{asset_class}/stage/update")
def update_stage(
    asset_class: str,
    verified: bool = False,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege()
    stage_data(asset_class, verified)


@router.get("/assetClasses/{asset_class}/stage/data")
def get_stage_data(
    asset_class: str,
    verified: bool = False,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege()
    return select_data(asset_class, schema=stage_schema(verified))

