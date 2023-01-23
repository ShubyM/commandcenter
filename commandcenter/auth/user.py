from typing import Set

from pydantic import BaseModel
from starlette.authentication import BaseUser as StarletteBaseUser



class BaseUser(BaseModel, StarletteBaseUser):
    """Base model for all commandcenter users."""
    username: str
    first_name: str | None
    last_name: str | None
    email: str | None
    upi: int | None
    company: str | None
    country: str | None
    scopes: Set[str] | None

    @property
    def is_authenticated(self) -> bool:
        return True

    @property
    def identity(self) -> str:
        return self.username

    @property
    def display_name(self) -> str:
        return self.identity