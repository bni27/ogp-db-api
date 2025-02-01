import logging

from fastapi import APIRouter, Depends, status

from app.auth import AuthLevel, User, validate_api_key
from app.operations import update_prod
from app.routers.data import (
    asset_class,
    filesys,
    raw_table,
    # record,
    reference,
    stage_table,
)

from app.sql import prod_table
from app.table import DB_MGMT


router = APIRouter()
logger = logging.getLogger(__name__)

router.include_router(
    asset_class.router,
    prefix="/assetClass",
    dependencies=[Depends(validate_api_key)],
)
router.include_router(
    filesys.router,
    prefix="/file",
    dependencies=[Depends(validate_api_key)],
)
router.include_router(
    raw_table.router,
    prefix="/rawTable",
    dependencies=[Depends(validate_api_key)],
)
router.include_router(
    reference.router,
    prefix="/reference",
    dependencies=[Depends(validate_api_key)],
)
router.include_router(
    stage_table.router,
    prefix="/stageTable",
    dependencies=[Depends(validate_api_key)],
)


@router.get("/")
async def data(
    db: DB_MGMT,
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege(AuthLevel.READ)
    return db.select_from_table(prod_table(verified), "prod")


@router.post("/update", status_code=status.HTTP_204_NO_CONTENT)
async def update(
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege()
    update_prod(verified)
    return status.HTTP_204_NO_CONTENT
