"""Authentication/Authorization tools for commandcenter.

Authentication backends are designed to pluggable into starlette's
`AuthenticationMiddleware`. The default backend uses active directory for
authentication and authorization which requires the host to be joined to a
domain.

Available Backends
- Active directory
"""
from .abc import AbstractAuthenticationBackend, AbstractAuthenticationClient
from .backends import on_error
from .exceptions import AuthError, UserNotFound
from .models import BaseUser
from .scopes import requires
from .token import TokenHandler



__all__ = [
    "AbstractAuthenticationBackend",
    "AbstractAuthenticationClient",
    "on_error",
    "AuthError",
    "UserNotFound",
    "BaseUser",
    "requires",
    "TokenHandler",
]