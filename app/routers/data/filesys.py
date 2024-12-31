import logging
from pathlib import Path

from fastapi import APIRouter, Depends, File, UploadFile
from fastapi.responses import FileResponse

from app.auth import AuthLevel, User, validate_api_key
from app.filesys import build_raw_file_path


router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/{asset_class}/uploadFile")
def upload_file(
    asset_class: str,
    file: UploadFile = File(),
    verified: bool = True,
    overwrite: bool = False,
    authenticated_user: User = Depends(validate_api_key),
):
    # upload a raw data file to an Asset Class folder
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


@router.get("/{asset_class}/{file_name}/downloadFile")
def download_file(
    asset_class: str,
    file_name: str,
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
) -> FileResponse:
    # Download file of specified asset class and filename
    authenticated_user.check_privilege()
    file_path = build_raw_file_path(file_name, asset_class, verified)
    return FileResponse(file_path)


@router.delete("/{asset_class}/{file_name}")
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
