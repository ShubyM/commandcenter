from typing import Optional, Set

from pydantic import BaseModel
from starlette.authentication import BaseUser as StarletteBaseUser



class BaseUser(BaseModel, StarletteBaseUser):
    """Base model for all commandcenter users.
    
    Implementations do not necessarily need to subclass this model the only
    requirement is that an implementation must subclass
    `starlette.authentication.BaseUser` the user information fields can vary
    based on the backend implementation.
    """
    username: str
    first_name: Optional[str]
    last_name: Optional[str]
    email: Optional[str]
    upi: Optional[int]
    company: Optional[str]
    country: Optional[str]
    scopes: Optional[Set[str]]