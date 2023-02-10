import functools
from typing import List

from starlette.middleware import Middleware
from starlette.middleware.authentication import AuthenticationMiddleware
from starlette.middleware.cors import CORSMiddleware

from commandcenter.config import CC_DEBUG_MODE
from commandcenter.config.middleware import (
    CC_MIDDLEWARE_CORS_ORIGINS,
    CC_MIDDLEWARE_ENABLE_CONTEXT,
    CC_MIDDLEWARE_ENABLE_CORS
)
from commandcenter.config.scopes import ADMIN_USER
from commandcenter.middleware import (
    CorrelationIDMiddleware,
    IPAddressMiddleware,
    UserMiddleware
)
from commandcenter.setup.auth import setup_auth_backend
from commandcenter.auth import on_error
from commandcenter.auth.debug import DebugAuthenticationMiddleware



def configure_auth_middleware(stack: List[Middleware]) -> None:
    """Configure authentication middleware and add to the stack.
    
    This must be run in the same thread as the event loop.
    """
    if CC_DEBUG_MODE:
        DebugAuthenticationMiddleware.set_user(ADMIN_USER)
        middleware = DebugAuthenticationMiddleware
    else:
        middleware = AuthenticationMiddleware
    backend = setup_auth_backend()
    partial = functools.partial(
        middleware,
        backend=backend,
        on_error=on_error
    )
    partial.__name__ = middleware.__name__
    stack.append(Middleware(partial))


def configure_context_middleware(stack: List[Middleware]) -> None:
    """Add context middleware to the stack."""
    stack.append(Middleware(CorrelationIDMiddleware))
    stack.append(Middleware(IPAddressMiddleware))
    stack.append(Middleware(UserMiddleware))


def configure_cors_middleware(stack: List[Middleware]) -> None:
    """Add CORS middlware to the stack."""
    origins = list(CC_MIDDLEWARE_CORS_ORIGINS) or ["*"]
    stack.append(
            Middleware(
                CORSMiddleware,
                allow_origins=origins,
                allow_credentials=True,
                allow_methods=["*"],
                allow_headers=["*"]
            )
        )


def configure_middleware() -> List[Middleware]:
    """Return the default middleware stack."""
    stack = []
    if CC_MIDDLEWARE_ENABLE_CORS:
        configure_cors_middleware(stack=stack)
    configure_auth_middleware(stack=stack)
    if CC_MIDDLEWARE_ENABLE_CONTEXT:
        configure_context_middleware(stack=stack)
    return stack