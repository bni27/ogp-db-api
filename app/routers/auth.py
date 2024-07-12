from time import mktime
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from app.auth import generate_api_key, validate_api_key
from app.user import AuthLevel, User


router = APIRouter()


class APIGenRequest(BaseModel):
    name: str
    auth_level: str
    exp_date: str


@router.get("/")
async def generate_key(
    gen_req: APIGenRequest, api_key: User = Depends(validate_api_key)
):
    if api_key.auth_level != AuthLevel.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You lack sufficient privileges for this action.",
        )
    exp_date = mktime(
        datetime.strptime(gen_req.exp_date, "%Y-%m-%d").date().timetuple()
    )
    key = generate_api_key(gen_req.name, gen_req.auth_level, exp_date)
    return {"key": key}
