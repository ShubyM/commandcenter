from starlette.middleware.authentication import AuthenticationMiddleware
from starlette.types import Receive, Scope, Send

from commandcenter.auth.models import BaseUser


class DebugAuthMiddleware(AuthenticationMiddleware):
    """Authentication middleware for debug mode *ONLY*. This always return an
    admin user.
    """
    admin_user: BaseUser = None

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ["http", "websocket"]:
            await self.app(scope, receive, send)
            return
        scope["auth"], scope["user"] = self.admin_user.scopes, self.admin_user
        await self.app(scope, receive, send)