from enum import auto, StrEnum

from pydantic import BaseModel


class AuthLevel(StrEnum):
    ADMIN = auto()
    EDIT = auto()
    READ = auto()


class User(BaseModel):
    name: str
    auth_level: AuthLevel
    exp_date: int
