from typing import Any, Type

from fastapi import HTTPException, status

from commandcenter.core.util.cache import (
    AbstractTokenGenerator,
    ReferenceToken,
    cache_by_token
)



def get_cached_reference(obj: Type[AbstractTokenGenerator]) -> Any:
    """Dependency to recall a cached obj by a reference token."""
    def wrap(token: str) -> AbstractTokenGenerator:
        try:
            ret = cache_by_token(token)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Token was not found or it has expired."
            )
        if not isinstance(ret, obj):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Token does not reference a valid type."
            )
        return ret
    return wrap


def get_reference_token(obj: Type[AbstractTokenGenerator]):
    """Dependency to cache an object and produce a refrence token."""
    def wrap(o: obj) -> ReferenceToken:
        token = o.token
        cache_by_token(token.token, o)
        return token
    return wrap