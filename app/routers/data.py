import logging
from os.path import exists, join, isdir, mkdir
from pathlib import Path

from fastapi import APIRouter, Depends, File, status, UploadFile

from app.db import load_raw, select_data, union_prod
from app.auth import AuthLevel, User, validate_api_key

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/")
async def data(asset_class: str | None = None):
    return select_data(asset_class)


@router.post("/uploadFile")
def upload_file(
    asset_class: str,
    file: UploadFile = File(),
    verified: bool = False,
    overwrite: bool = False,
    authenticated_user: User = Depends(validate_api_key),
):
    bucket_mount: str = "/data"
    required_perm = AuthLevel.ADMIN if (verified or overwrite) else AuthLevel.EDIT
    authenticated_user.check_privilege(required_perm)
    ver_folder = "verified" if verified else "unverified"
    base_path = join(bucket_mount, ver_folder, asset_class)
    if not isdir(base_path):
        mkdir(base_path)
    file_path = join(base_path, file.filename)
    if exists(file_path) and not overwrite:
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


@router.post('/updateRaw')
def update_raw():
    file_path = Path(
        '/data/verified/batteries_electrolyzer/batteries_electrolyzer.csv'
    )
    try:
        load_raw(file_path)
    except Exception as e:
        logger.exception(e)


@router.post("/updateVerified", status_code=status.HTTP_204_NO_CONTENT)
async def update(authenticated_user: User = Depends(validate_api_key)):
    authenticated_user.check_privilege()
    # load new raw datasets
    # process them to stage
    # union them to prod table
    union_prod()
    return
