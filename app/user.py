from enum import auto, IntEnum

from fastapi import HTTPException, status
from pydantic import BaseModel


class AuthLevel(IntEnum):
    ADMIN = auto()
    EDIT = auto()
    READ = auto()


class User(BaseModel):
    name: str
    auth_level: AuthLevel
    exp_date: int

    def check_privilege(
        self, required_permission: AuthLevel = AuthLevel.ADMIN
    ) -> None:
        if self.auth_level > required_permission:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You lack sufficient privileges for this action.",
            )
