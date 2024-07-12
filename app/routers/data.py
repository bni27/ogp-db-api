from fastapi import APIRouter, Depends, HTTPException, status

from app.db import select_data, union_prod
from app.auth import AuthLevel, User, validate_api_key

router = APIRouter()


@router.get("/")
async def data(asset_class: str | None = None):
    return select_data(asset_class)


@router.post("/update", status_code=status.HTTP_204_NO_CONTENT)
async def update(api_key: User = Depends(validate_api_key)):
    if api_key.auth_level != AuthLevel.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You lack sufficient privileges for this action.",
        )
    # load new raw datasets
    # process them to stage
    # union them to prod table
    union_prod()
    return
