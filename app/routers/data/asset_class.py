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
from app.table import DB_MGMT


router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/")
def get_asset_classes(verified: bool = True):
    # Get a list of all available asset classes
    return {
        "verification_status": "verified" if verified else "unverified",
        "asset_classes": [d.stem for d in get_directories(verified)],
    }


@router.post("/{asset_class}")
def create_asset_class(
    asset_class: str,
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
):
    # Create an asset class
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


@router.delete("/{asset_class}")
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


@router.get("/{asset_class}/files")
def get_asset_class_files(
    asset_class: str,
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
):
    # List all uploaded raw data files belonging to an asset class
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
