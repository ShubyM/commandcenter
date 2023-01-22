from abc import ABC, abstractproperty
from typing import Any, Optional

from pydantic import BaseModel

from commandcenter.core.objcache import memo



class ReferenceToken(BaseModel):
    """Token that can be cached along with an object to reference the object."""
    token: str


class AbstractTokenGenerator(ABC, BaseModel):
    """A base model object that can return a reference token stable across runtimes."""
    @abstractproperty
    def token(self) -> ReferenceToken:
        raise NotImplementedError()


@memo(persist=True)
def cache_by_token(
    token: str,
    _obj: Optional[Any] = None
) -> Any:
    """Utilizes the mechanics of the memo cache to pass both a token and obj
    and then recall the obj by just passing the token.

    Raises:
        ValueError: If token is not valid.
    """
    if not _obj:
        raise ValueError()
    return _obj