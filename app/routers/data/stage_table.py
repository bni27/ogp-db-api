import logging

from fastapi import APIRouter, Depends, HTTPException, status

from app.auth import User, validate_api_key
from app.operations import stage_data
from app.sql import stage_schema
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
    stage_data(asset_class, db, verified)
    return


@router.delete("/assetClasses/{asset_class}/stage")
def delete_stage(
    asset_class: str,
    db: DB_MGMT,
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege()
    db.drop_table(asset_class, stage_schema(verified))
    return status.HTTP_204_NO_CONTENT


@router.get("/assetClasses/{asset_class}/stage/data")
def get_stage_data(
    asset_class: str,
    db: DB_MGMT,
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege()
    return db.select_from_table(asset_class, schema=stage_schema(verified))


@router.get("/{table_name}/record")
def get_staged_record(
    asset_class: str,
    db: DB_MGMT,
    project_id: str,
    sample: str,
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege()
    try:
        return db.select_by_id(asset_class, stage_schema(verified), project_id, sample)
    except StopIteration:
        return HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project with project_id: {project_id}, and sample: {sample} could not be found in table: {stage_schema(verified)}.{asset_class}",
        )
