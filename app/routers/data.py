from os.path import join

from fastapi import APIRouter, Depends, File, HTTPException, status, UploadFile

from app.db import select_data, union_prod
from app.auth import AuthLevel, User, validate_api_key

router = APIRouter()


@router.get("/")
async def data(asset_class: str | None = None):
    return select_data(asset_class)


@router.post("/uploadFile")
def upload_file(
    file: UploadFile = File(),
    api_key: User = Depends(validate_api_key),
):
    bucket_mount: str = "/data"
    if api_key.auth_level != AuthLevel.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You lack sufficient privileges for this action.",
        )
    try:
        contents = file.file.read()
        with open(join(bucket_mount, file.filename), "wb") as f:
            f.write(contents)
    except Exception:
        return {"message": "There was an error uploading the file"}
    finally:
        file.file.close()

    return {"message": f"Successfully uploaded {file.filename}"}


@router.post("/update", status_code=status.HTTP_204_NO_CONTENT)
async def update(api_key: User = Depends(validate_api_key)):
    # load new raw datasets
    # process them to stage
    # union them to prod table
    union_prod()
    return
