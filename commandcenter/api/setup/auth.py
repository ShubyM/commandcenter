import functools
from typing import Type

from commandcenter.auth import AuthBackends
from commandcenter.auth.base import BaseAuthenticationBackend, TokenHandler
from commandcenter.auth.protocols import AuthenticationClient
from commandcenter.caching import singleton
from commandcenter.config.auth import (
    CC_AUTH_ALGORITHM,
    CC_AUTH_BACKEND,
    CC_AUTH_SECRET_KEY,
    CC_AUTH_TOKEN_EXPIRE
)


def inject_backend_dependencies(func) -> BaseAuthenticationBackend:
    """Wrapper around the auth backend setup that allows for dynamic configuration."""
    backend = CC_AUTH_BACKEND
    client = None
    # We make everything **kwargs instead of *args because the objcache decorator
    # can properly hash kwargs according to the argument name
    inject_kwargs = {}
    if backend is AuthBackends.ACTIVE_DIRECTORY.cls:
        from commandcenter.config.auth.backends.activedirectory import (
            CC_AUTH_BACKENDS_AD_DOMAIN,
            CC_AUTH_BACKENDS_AD_HOSTS,
            CC_AUTH_BACKENDS_AD_MAXCONN,
            CC_AUTH_BACKENDS_AD_MECHANISM,
            CC_AUTH_BACKENDS_AD_PASSWORD,
            CC_AUTH_BACKENDS_AD_TLS,
            CC_AUTH_BACKENDS_AD_USERNAME
        )
        from commandcenter.auth.backends.activedirectory import ActiveDirectoryClient
        client = ActiveDirectoryClient
        inject_kwargs.update(
            {
                "domain": CC_AUTH_BACKENDS_AD_DOMAIN,
                "hosts": list(CC_AUTH_BACKENDS_AD_HOSTS),
                "tls": CC_AUTH_BACKENDS_AD_TLS,
                "maxconn": CC_AUTH_BACKENDS_AD_MAXCONN,
                "username": CC_AUTH_BACKENDS_AD_USERNAME,
                "password": str(CC_AUTH_BACKENDS_AD_PASSWORD),
                "mechanism": CC_AUTH_BACKENDS_AD_MECHANISM
            }
        )
    else:
        raise RuntimeError("Received invalid backend.")

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(backend, client, **inject_kwargs)
    
    return wrapper


@inject_backend_dependencies
@singleton
def setup_auth_backend(
    backend: Type[BaseAuthenticationBackend],
    client: Type[AuthenticationClient],
    **kwargs
) -> BaseAuthenticationBackend:
    """Initialize auth client for backend and setup backend.
    
    This must be run in the same thread as the event loop.
    """
    handler = TokenHandler(
        key=str(CC_AUTH_SECRET_KEY),
        expire=CC_AUTH_TOKEN_EXPIRE,
        algorithm=CC_AUTH_ALGORITHM
    )
    client = client(**kwargs)
    return backend(handler, client)