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
from app.filesys import (
    build_asset_path,
    build_raw_file_path,
    get_data_files,
    get_directories,
)
from app.pg import Record, row_count, select_data
from app.pg import DateFormatError, DuplicateHeaderError, PrimaryKeysMissingError
from app.sql import prod_table, stage_schema
from app.operations import load_exchange_rate, load_ppp_rate, load_gdp_deflators
from app.table import DB_MGMT


router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/reference/exchangeRates/update")
def update_exchange_rate(
    db: DB_MGMT,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege()
    load_exchange_rate(db)
    


@router.post("/reference/ppp/update")
def update_ppp_rate(
    db: DB_MGMT,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege()
    load_ppp_rate(db)


@router.post("/reference/gdpDeflators/update")
def update_gdp_deflator_rate(
    db: DB_MGMT,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege()
    load_gdp_deflators(db)
