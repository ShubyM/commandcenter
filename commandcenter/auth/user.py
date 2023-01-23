from typing import Optional, Set

from pydantic import BaseModel
from starlette.authentication import BaseUser as StarletteBaseUser



class BaseUser(BaseModel, StarletteBaseUser):
    """Base model for all commandcenter users."""
    username: str
    first_name: Optional[str]
    last_name: Optional[str]
    email: Optional[str]
    upi: Optional[int]
    company: Optional[str]
    country: Optional[str]
    scopes: Optional[Set[str]]

    @property
    def is_authenticated(self) -> bool:
        return True

    @property
    def identity(self) -> str:
        return self.username

    @property
    def display_name(self) -> str:
        return self.identity