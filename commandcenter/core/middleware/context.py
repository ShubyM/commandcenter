from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from commandcenter.core.util.context import ip_address_context, user_context



class IPAddressMiddleware(BaseHTTPMiddleware):
    """Middleware that sets the ip address context variable for logging.
    
    If working behind a proxy, be sure to set the '--proxy-headers' on uvicorn.
    """
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        """Assign the user value from `request.client.host` to the context variable."""
        ip_address = request.client.host
        if ip_address:
            token = ip_address_context.set(ip_address)
            try:
                return await call_next(request)
            finally:
                ip_address_context.reset(token)
        else:
            return await call_next(request)


class UserMiddleware(BaseHTTPMiddleware):
    """Middleware that sets the user context variable for logging.
    
    The `AuthenticationMiddleware` must be installed to use this middleware.
    """
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        """Assign the user value from `request.user` to the context variable."""
        user = request.user
        if user.is_authenticated:
            token = user_context.set(user)
            try:
                return await call_next(request)
            finally:
                user_context.reset(token)
        else:
            return await call_next(request)