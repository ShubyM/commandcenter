from starlette.authentication import AuthenticationError
from starlette.requests import HTTPConnection
from starlette.responses import JSONResponse, Response

from commandcenter.util import ObjSelection
from .activedirectory import ActiveDirectoryBackend



__all__ = ["AuthBackends", "on_error"]


class AuthBackends(ObjSelection):
    DEFAULT = "default", ActiveDirectoryBackend
    ACTIVE_DIRECTORY = "activedirectory", ActiveDirectoryBackend


def on_error(conn: HTTPConnection, err: AuthenticationError) -> Response:
    """An error handler function for the `AuthenticationMiddleware`."""
    return JSONResponse(
        {"detail": f"The request cannot be completed. {str(err)}"},
        status_code=500
    )