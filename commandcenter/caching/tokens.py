from typing import Any

from pydantic import BaseModel

from commandcenter.caching.memo import memo



class ReferenceToken(BaseModel):
    """Token that can be cached along with an object to reference the object."""
    token: str


class Tokenable(BaseModel):
    """Standard model for objects that can be cached by a reference token."""
    @property
    def token(self) -> ReferenceToken:
        """Return a token uniquely identifying this model."""
        raise NotImplementedError()


@memo
def cache_tokenable(
    token: str,
    _obj: Tokenable | None = None
) -> Any:
    """Utilizes the mechanics of the memo cache to pass both a token and tokenable
    object and then recall the object by just passing the token.

    Note: The object must be pickleable.

    Raises:
        ValueError: Invalid token.
    """
    if not _obj:
        raise ValueError("Invalid token.")
    return _obj