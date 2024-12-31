import logging

from fastapi import APIRouter, Depends, status

from app.auth import User, validate_api_key
from app.db import (
    union_prod,
)
from app.pg import select_data
from app.sql import prod_table
from app.table import DB_MGMT
from app.routers.data import (
    asset_class,
    filesys,
    raw_table,
    # record,
    # reference,
    # stage_table,
)


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
# router.include_router(
#     record.router,
#     prefix="/record",
#     dependencies=[Depends(validate_api_key)],
# )
# router.include_router(
#     reference.router,
#     prefix="/reference",
#     dependencies=[Depends(validate_api_key)],
# )
# router.include_router(
#     stage_table.router,
#     prefix="/stageTable",
#     dependencies=[Depends(validate_api_key)],
# )


@router.get("/")
async def data(db: DB_MGMT, verified: bool = True):
    return select_data(prod_table(verified), "prod")


@router.post("/update", status_code=status.HTTP_204_NO_CONTENT)
async def update(
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege()
    union_prod(verified)
