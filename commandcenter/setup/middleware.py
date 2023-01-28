import functools
from typing import List

from starlette.middleware import Middleware
from starlette.middleware.authentication import AuthenticationMiddleware
from starlette.middleware.cors import CORSMiddleware

from commandcenter.auth import on_error
from commandcenter.auth.debug import DebugAuthMiddleware
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



def setup_auth_middleware(stack: List[Middleware]) -> None:
    """Configure authentication middleware and add to the stack.
    
    This must be run in the same thread as the event loop.
    """
    if CC_DEBUG_MODE:
        DebugAuthMiddleware.admin_user = ADMIN_USER
        middleware = DebugAuthMiddleware
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


def setup_context_middleware(stack: List[Middleware]) -> None:
    """Add context middleware to the stack."""
    stack.append(Middleware(CorrelationIDMiddleware))
    stack.append(Middleware(IPAddressMiddleware))
    stack.append(Middleware(UserMiddleware))


def setup_cors_middleware(stack: List[Middleware]) -> None:
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


def setup_middleware() -> List[Middleware]:
    """Configure a FastAPI application instance with the enabled middleware."""
    stack = []
    if CC_MIDDLEWARE_ENABLE_CORS:
        setup_cors_middleware(stack=stack)
    setup_auth_middleware(stack=stack)
    if CC_MIDDLEWARE_ENABLE_CONTEXT:
        setup_context_middleware(stack=stack)
    return list(reversed(stack))