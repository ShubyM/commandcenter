from typing import Tuple

from ratelimit.auths import EmptyInformation
from starlette.authentication import BaseUser
from starlette.types import Scope



def username(scope: Scope) -> Tuple[str, str]:
    """Auth backend for rate limiting based on username.
    
    This requires the 'user' key in the scope, therefore the `AuthenticationMiddleware`
    must be installed in the stack before the rate limit middleware.
    """
    user: BaseUser = scope["user"]
    if scope["user"]:
        id_ = user.identity
        if id_:
            return id_, "default"
    raise EmptyInformation(scope)