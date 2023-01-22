import itertools
from typing import List

from fastapi.middleware import Middleware
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.authentication import AuthenticationMiddleware
from starlette.types import Receive, Scope, Send

from commandcenter.common.auth import setup_auth_middleware
from commandcenter.config import CC_DEBUG_MODE
from commandcenter.config.middleware import (
    CC_MIDDLEWARE_CORS_ORIGINS,
    CC_MIDDLEWARE_ENABLE_CONTEXT,
    CC_MIDDLEWARE_ENABLE_CORS
)
from commandcenter.config.scopes import (
    CC_SCOPES_ADMIN_ACCESS,
    CC_SCOPES_PIWEB_ACCESS,
    CC_SCOPES_TRAXX_ACCESS
)
from commandcenter.core.auth import BaseUser
from commandcenter.core.middleware import (
    CorrelationIDMiddlewareMod,
    IPAddressMiddleware,
    UserMiddleware
)



class DebugUser(BaseUser):
    @property
    def is_authenticated(self) -> bool:
        return True
    
    @property
    def identity(self) -> str:
        return self.username

    @property
    def display_name(self) -> str:
        return f"{self.last_name}, {self.first_name}"


DEBUG_USER = DebugUser(
    username="admin",
    first_name="Christopher",
    last_name="Newville",
    email="chrisnewville1396@gmail.com",
    upi=2191996,
    company="Prestige Worldwide",
    scopes=set(
        itertools.chain(
            list(CC_SCOPES_ADMIN_ACCESS),
            list(CC_SCOPES_PIWEB_ACCESS),
            list(CC_SCOPES_TRAXX_ACCESS)
        )
    )
)


class DebugAuthMiddleware(AuthenticationMiddleware):
    """Authentication middleware for debug mode *ONLY*. This always return an
    admin user.
    """
    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ["http", "websocket"]:
            await self.app(scope, receive, send)
            return
        scope["auth"], scope["user"] = DEBUG_USER.scopes, DEBUG_USER
        await self.app(scope, receive, send)


def setup_middleware() -> List[Middleware]:
    """Configure a FastAPI application instance with the enabled middleware.
    
    Note: Any additional middleware for the application instance should be added
    before calling this method.
    """
    middleware = []
    if CC_MIDDLEWARE_ENABLE_CONTEXT:
        middleware.insert(0, Middleware(UserMiddleware))
        middleware.insert(0, Middleware(IPAddressMiddleware))
        middleware.insert(0, Middleware(CorrelationIDMiddlewareMod))
        
    if CC_DEBUG_MODE:
        auth_middleware = setup_auth_middleware(DebugAuthMiddleware)
    else:
        auth_middleware = setup_auth_middleware(AuthenticationMiddleware)
    
    middleware.insert(0, Middleware(auth_middleware))

    if CC_MIDDLEWARE_ENABLE_CORS:
        origins = list(CC_MIDDLEWARE_CORS_ORIGINS) or ["*"]
        middleware.insert(
            0,
            Middleware(
                CORSMiddleware,
                allow_origins=origins,
                allow_credentials=True,
                allow_methods=["*"],
                allow_headers=["*"]
            )
        )

    return middleware