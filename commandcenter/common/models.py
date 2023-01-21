from enum import Enum

from pydantic import BaseModel



class Token(BaseModel):
    """Access token model."""
    access_token: str
    token_type: str


class StatusOptions(str, Enum):
    OK = "OK"
    FAILED = "FAILED"


class Status(BaseModel):
    """Model for operation status responses."""
    status: StatusOptions