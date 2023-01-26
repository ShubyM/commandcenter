from typing import Any, Type

from fastapi import HTTPException, status

from commandcenter.caching.tokens import ReferenceToken, Tokenable, cache_tokenable



def get_cached_reference(obj: Type[Tokenable], raise_on_miss: bool = True) -> Any:
    """Dependency to recall a cached obj by a reference token."""
    def wrap(token: str) -> Tokenable:
        try:
            ret = cache_tokenable(token)
        except ValueError:
            if raise_on_miss:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Token was not found or it has expired."
                )
            return
        print(type(ret))
        if not isinstance(ret, obj):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Token does not reference a valid type."
            )
        return ret
    return wrap


def get_reference_token(obj: Type[Tokenable]):
    """Dependency to cache an object and produce a refrence token."""
    def wrap(o: obj) -> ReferenceToken:
        token = o.token
        cache_tokenable(token.token, o)
        return token
    return wrap