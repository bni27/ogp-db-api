import logging

from fastapi import APIRouter, Depends, status

from app.auth import User, validate_api_key
from app.operations import load_exchange_rate, load_ppp_rate, load_gdp_deflators
from app.db import DB_MGMT


router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/reference/exchangeRates/update")
def update_exchange_rate(
    db: DB_MGMT,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege()
    load_exchange_rate(db)
    return status.HTTP_204_NO_CONTENT


@router.post("/reference/ppp/update")
def update_ppp_rate(
    db: DB_MGMT,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege()
    load_ppp_rate(db)
    return status.HTTP_204_NO_CONTENT


@router.post("/reference/gdpDeflators/update")
def update_gdp_deflator_rate(
    db: DB_MGMT,
    authenticated_user: User = Depends(validate_api_key),
):
    authenticated_user.check_privilege()
    load_gdp_deflators(db)
    return status.HTTP_204_NO_CONTENT
