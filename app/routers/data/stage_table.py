import logging
from pathlib import Path

from fastapi import APIRouter, Depends, File, HTTPException, status, UploadFile
from fastapi.responses import FileResponse

from app.auth import AuthLevel, User, validate_api_key
from app.filesys import (
    get_data_files,
)
from app.operations import stage_data
from app.pg import Record, row_count, select_data
from app.pg import DateFormatError, DuplicateHeaderError, PrimaryKeysMissingError
from app.sql import prod_table, stage_schema
from app.table import DB_MGMT


router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/{asset_class}/update")
def update_stage(
    asset_class: str,
    db: DB_MGMT,
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege()
    return [r for r in stage_data(asset_class, db, verified)]


# @router.delete("/assetClasses/{asset_class}/stage")
# def delete_stage(
#     asset_class: str,
#     verified: bool = True,
#     authenticated_user: User = Depends(validate_api_key),
# ):
#     authenticated_user.check_privilege()
#     delete_stage_table(asset_class, verified)
#     return status.HTTP_204_NO_CONTENT


# @router.get("/assetClasses/{asset_class}/stage/data")
# def get_stage_data(
#     asset_class: str,
#     verified: bool = True,
#     authenticated_user: User = Depends(validate_api_key),
# ):
#     authenticated_user.check_privilege()
#     return select_data(asset_class, schema=stage_schema(verified))