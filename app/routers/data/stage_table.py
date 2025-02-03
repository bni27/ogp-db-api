import logging

from fastapi import APIRouter, Depends, HTTPException, status

from app.auth import User, validate_api_key
from app.operations import stage_data, stage_schema
from app.db import DB_MGMT


router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/{asset_class}/update")
def update_stage(

    asset_class: str,
    db: DB_MGMT,
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
):
    """
    Update the stage table with the given asset class.

    Args:
        asset_class (str): The asset class to be staged.
        db (DB_MGMT): The database management object.
        verified (bool, optional): Flag indicating if the data is verified. Defaults to True.
        authenticated_user (User, optional): The authenticated user performing the update. Defaults to Depends(validate_api_key).

    Returns:
        None
    """
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
    """
    Delete a stage table for a given asset class.

    Args:
        asset_class (str): The asset class for which the stage table should be deleted.
        db (DB_MGMT): The database management instance to interact with the database.
        verified (bool, optional): Flag indicating whether the table is verified. Defaults to True.
        authenticated_user (User, optional): The authenticated user performing the operation. Defaults to Depends(validate_api_key).

    Returns:
        int: HTTP status code 204 indicating successful deletion.
    """
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
    """
    Retrieve a staged record from the database.

    Args:
        asset_class (str): The class of the asset.
        db (DB_MGMT): The database management instance.
        project_id (str): The ID of the project.
        sample (str): The sample identifier.
        verified (bool, optional): Flag indicating if the record is verified. Defaults to True.
        authenticated_user (User, optional): The authenticated user. Defaults to Depends(validate_api_key).

    Returns:
        dict: The staged record if found.

    Raises:
        HTTPException: If the record is not found, raises a 404 HTTP exception.
    """
    authenticated_user.check_privilege()
    try:
        return db.select_by_id(asset_class, stage_schema(verified), project_id, sample)
    except StopIteration:
        return HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project with project_id: {project_id}, and sample: {sample} could not be found in table: {stage_schema(verified)}.{asset_class}",
        )
