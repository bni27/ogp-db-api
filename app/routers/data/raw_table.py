import logging
from pathlib import Path

from fastapi import APIRouter, Depends, File, HTTPException, status, UploadFile
from fastapi.responses import FileResponse

from app.auth import AuthLevel, User, validate_api_key
from app.filesys import (
    build_asset_path,
    build_raw_file_path,
    get_data_files,
    get_directories,
)
from app.operations import load_raw_data, drop_raw_table
from app.pg import Record, row_count, select_data
from app.pg import DateFormatError, DuplicateHeaderError, PrimaryKeysMissingError
from app.sql import prod_table, raw_schema, stage_schema
from app.table import DB_MGMT

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/")
def get_raw_tables(
    db: DB_MGMT,
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
):
    """
    Get a list of all raw tables.
    """
    authenticated_user.check_privilege()
    return {"tables": db.get_all_table_names(raw_schema(verified))}


@router.post("/{asset_class}/{file_name}/load")
def load_raw(
    asset_class: str,
    file_name: str,
    db: DB_MGMT,
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
):
    """
    Load file into Raw Table
    """
    authenticated_user.check_privilege()
    try:
        file_path = build_raw_file_path(file_name, asset_class, verified)
    except ValueError:
        return HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Asset class '{asset_class}' does not exist",
        )
    logger.info(f"Loading raw file: {file_path} into database.")
    try:
        table = load_raw_data(file_path, db)
    except FileNotFoundError:
        return HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"File '{file_name}' in asset class '{asset_class}' does not exist",
        )
    except DuplicateHeaderError:
        return HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Duplicate headers found. Please fix and re-upload the file.",
        )
    except PrimaryKeysMissingError:
        return HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Required primary keys missing. Make sure file has 'project_id' and 'sample' fields.",
        )
    except DateFormatError:
        return HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Date field was uploaded with improper format. Double check that your date fields are 'YYYY-MM-DD'",
        )
    except Exception as e:
        logger.exception(e)
        return HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e)
        )
    return {
        "table_name": table,
    }


# @router.post("/{table_name}")
# def create_raw_table(headers):
#     # Initialize new raw table with no records
#     pass


@router.delete("/{table_name}")
def delete_raw(
    table_name: str,
    db: DB_MGMT,
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
):
    """Delete Raw Table
    """
    authenticated_user.check_privilege()
    drop_raw_table(table_name, verified, db)
    return status.HTTP_204_NO_CONTENT