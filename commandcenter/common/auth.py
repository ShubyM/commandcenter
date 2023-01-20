import functools
import inspect
from typing import Callable, Type

from starlette.middleware.authentication import AuthenticationMiddleware
from starlette.types import ASGIApp

from commandcenter.config.auth import (
    CC_AUTH_ALGORITHM,
    CC_AUTH_BACKEND,
    CC_AUTH_SECRET_KEY,
    CC_AUTH_TOKEN_EXPIRE
)
from commandcenter.core.auth import (
    AbstractAuthenticationBackend,
    AbstractAuthenticationClient,
    TokenHandler
)
from commandcenter.core.auth.backends import AvailableBackends, on_error
from commandcenter.core.objcache import singleton



def inject_backend_dependencies(func) -> AbstractAuthenticationBackend:
    """Wrapper around the auth backend setup that allows for dynamic injection 
    of the arguments based on the environment at runtime.
    """
    backend = CC_AUTH_BACKEND
    client = None
    # We make everything **kwargs instead of *args because the objcache decorator
    # can properly hash kwargs according to the argument name
    inject_kwargs = {}
    if backend is AvailableBackends.AD.cls:
        from commandcenter.config.auth.backends.ad import (
            CC_AUTH_BACKENDS_AD_DOMAIN,
            CC_AUTH_BACKENDS_AD_HOSTS,
            CC_AUTH_BACKENDS_AD_MAXCONN,
            CC_AUTH_BACKENDS_AD_TLS
        )
        from commandcenter.core.auth.backends.ad import ActiveDirectoryClient
        client = ActiveDirectoryClient
        inject_kwargs.update(
            {
                "domain": CC_AUTH_BACKENDS_AD_DOMAIN,
                "dc_hosts": list(CC_AUTH_BACKENDS_AD_HOSTS),
                "maxconn": CC_AUTH_BACKENDS_AD_MAXCONN,
                "tls": CC_AUTH_BACKENDS_AD_TLS
            }
        )
    else:
        raise RuntimeError("Received invalid backend.")

    @functools.wraps(func)
    async def async_wrapper(*args, **kwargs):
        return await func(backend, client, **inject_kwargs)
    
    return async_wrapper


@inject_backend_dependencies
@singleton
async def setup_auth_backend(
    backend: Type[AbstractAuthenticationBackend],
    client: Type[AbstractAuthenticationClient],
    **kwargs
) -> AbstractAuthenticationBackend:
    """Initialize auth client for backend and setup backend."""
    handler = TokenHandler(
        key=str(CC_AUTH_SECRET_KEY),
        expire=CC_AUTH_TOKEN_EXPIRE,
        algorithm=CC_AUTH_ALGORITHM
    )
    client = client(**kwargs)
    return backend(handler, client)


async def setup_auth_middleware() -> Callable[[ASGIApp], AuthenticationMiddleware]:
    """Configure authentication middleware with backend from config."""
    backend = await setup_auth_backend()
    return functools.partial(
        AuthenticationMiddleware,
        backend=backend,
        on_error=on_error
    )


async def get_auth_client() -> AbstractAuthenticationClient:
    """Retrieve the auth client from the authentication backend."""
    backend: AbstractAuthenticationBackend = await setup_auth_backend()
    return backend.client


async def get_token_handler() -> TokenHandler:
    """Retrieve the token handler from the authentication backend."""
    backend: AbstractAuthenticationBackend = await setup_auth_backend()
    return backend.handler