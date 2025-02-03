import logging

from fastapi import APIRouter, Depends

from app.auth import AuthLevel, User, validate_api_key

from app.operations import prod_table, stage_schema

from app.db import DB_MGMT

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/{asset_class}/{field_name}")
def get_rcf_curve(
    asset_class: str,
    field_name: str,
    db: DB_MGMT,
    num_intervals: int = 20,
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege(AuthLevel.READ)
    db.map_existing_table(prod_table(), "prod")
    


@router.get("/{asset_class}/available_fields")
def get_available_fields(
    asset_class: str,
    db: DB_MGMT,
    verified: bool = True,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege(AuthLevel.READ)
    return [
        c.name
        for c in db.get_columns(asset_class, stage_schema(verified))
        if c.name.endswith("_ratio")
    ]
