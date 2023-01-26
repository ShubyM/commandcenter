from enum import Enum

from pydantic import BaseModel



class StatusOptions(str, Enum):
    OK = "OK"
    FAILED = "FAILED"


class Status(BaseModel):
    """Model for operation status responses."""
    status: StatusOptions