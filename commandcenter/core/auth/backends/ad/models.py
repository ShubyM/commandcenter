from typing import Set

from pydantic import Field

from commandcenter.core.auth.models import BaseUser



class LDAPItem(str):
    """Custom type for single index list entries from a `LDAPEntry`.
    
    This extracts the first index as a string and returns it.
    """
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not isinstance(v, list):
            raise TypeError("Expected list")
        if len(v) > 1:
            raise ValueError(f"Too many values for {cls.__name__}. Expected 1, got {len(v)}")
        elif len(v) < 1:
            return ""
        return v[0]


class ActiveDirectoryUser(BaseUser):
    """Active directory user model."""
    username: LDAPItem = Field(alias="cn")
    first_name: LDAPItem = Field(alias="givenName")
    last_name: LDAPItem = Field(alias="sn")
    email: LDAPItem = Field(alias="mail", default=None)
    upi: LDAPItem = Field(alias="employeeNumber", default=None)
    company: LDAPItem = Field(default=None)
    country: LDAPItem = Field(alias="c", default=None)
    groups: Set[str]

    @property
    def is_authenticated(self) -> bool:
        return True
    
    @property
    def display_name(self) -> str:
        return f"{self.first_name} {self.last_name}"

    @property
    def identity(self) -> str:
        return self.username