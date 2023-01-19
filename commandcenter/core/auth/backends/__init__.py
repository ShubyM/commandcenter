from starlette.authentication import AuthenticationError
from starlette.requests import HTTPConnection
from starlette.responses import JSONResponse, Response

from commandcenter.core.util.enums import ObjSelection
from .ad import ActiveDirectoryBackend



def on_error(conn: HTTPConnection, err: AuthenticationError) -> Response:
    """An error handler function for the `AuthenticationMiddleware`."""
    return JSONResponse(
        {"detail": f"The request cannot be completed. {str(err)}"},
        status_code=500
    )


class AvailableBackends(ObjSelection):
    DEFAULT = "ad", ActiveDirectoryBackend
    AD = "ad", ActiveDirectoryBackend


__all__ = [
    "ActiveDirectoryBackend"
]