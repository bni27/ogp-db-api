import logging

from fastapi import APIRouter, Depends, HTTPException, status

from app.auth import User, validate_api_key
from app.exception import DateFormatError, DuplicateHeaderError, PrimaryKeysMissingError
from app.filesys import (
    add_record_to_file,
    build_raw_file_path,
    delete_record_from_file,
    find_file,
    read_raw_data_file,
    update_record_in_file,
)
from app.column import column_details
from app.db import DB_MGMT, Record
from app.schema import raw_schema


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
        headers, data = read_raw_data_file(file_path)
        db.create_new_table(
            file_path.stem,
            raw_schema(verified),
            column_details(headers)
        )
        db.load_data_into_table(file_path.stem, raw_schema(verified), data)
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
        "table_name": file_path.stem,
    }


@router.get("/{table_name}")
def get_raw_table(
    table_name: str,
    db: DB_MGMT,
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
):
    """Return raw data from table."""
    authenticated_user.check_privilege()
    return db.select_from_table(table_name, raw_schema(verified))


@router.delete("/{table_name}")
def delete_raw(
    table_name: str,
    db: DB_MGMT,
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
):
    """Delete Raw Table"""
    authenticated_user.check_privilege()
    db.drop_table(table_name, raw_schema(verified))
    return status.HTTP_204_NO_CONTENT


@router.get("/{table_name}/record")
def get_raw_record(
    table_name: str,
    db: DB_MGMT,
    project_id: str,
    sample: str,
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege()
    try:
        return db.select_by_id(table_name, raw_schema(verified), project_id, sample)
    except StopIteration:
        return HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project with project_id: {project_id}, and sample: {sample} could not be found in table: {raw_schema(verified)}.{table_name}",
        )


@router.delete("/{table_name}/record")
def delete_raw_record(
    table_name: str,
    db: DB_MGMT,
    project_id: str,
    sample: str,
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege()
    try:
        record = db.select_by_id(table_name, raw_schema(verified), project_id, sample)
    except StopIteration:
        return HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project with project_id: {project_id}, and sample: {sample} could not be found in table: {raw_schema(verified)}.{table_name}",
        )

    with db.get_session() as session:
        try:
            session.delete(record)
            file_path = find_file(table_name, verified)
            delete_record_from_file(file_path, project_id, sample)
            session.commit()
        except:
            session.rollback()

    return status.HTTP_204_NO_CONTENT


@router.put("/{table_name}/record")
def update_raw_record(
    table_name: str,
    db: DB_MGMT,
    record: Record,
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege()
    try:
        row = db.select_by_id(
            table_name, raw_schema(verified), record.project_id, record.sample
        )
    except StopIteration:
        return HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project with project_id: {record.project_id}, and sample: {record.sample} could not be found in table: {raw_schema(verified)}.{table_name}",
        )
    with db.get_session() as session:
        try:
            row.sqlmodel_update(record.data)
            session.add(row)
            file_path = find_file(table_name, verified)
            update_record_in_file(
                file_path, record.project_id, record.sample, record.data
            )
            session.commit()
            session.refresh(row)
        except Exception as e:
            logger.error(
                f"Unable to update record in {raw_schema(verified)}.{table_name}"
            )
            logger.exception(e)
            session.rollback()
    return status.HTTP_204_NO_CONTENT


@router.post("/{table_name}/record")
def add_raw_record(
    table_name: str,
    db: DB_MGMT,
    record: Record,
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege()
    try:
        row = db.select_by_id(
            table_name, raw_schema(verified), record.project_id, record.sample
        )
        return HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Project with project_id: {record.project_id}, and sample: {record.sample} already exists in: {raw_schema(verified)}.{table_name}",
        )
    except StopIteration:
        pass
    with db.get_session() as session:
        try:
            row = db.tables[raw_schema(verified)][table_name](
                **{
                    **record.data,
                    "project_id": record.project_id,
                    "sample": record.sample,
                }
            )
            session.add(row)
            file_path = find_file(table_name, verified)
            add_record_to_file(file_path, record.project_id, record.sample, record.data)
            session.commit()
            session.refresh(row)
        except Exception as e:
            logger.error(
                f"Unable to update record in {raw_schema(verified)}.{table_name}"
            )
            logger.exception(e)
            session.rollback()
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    return status.HTTP_204_NO_CONTENT
