from typing import Set

from commandcenter.exceptions import CommandCenterException



class AuthError(CommandCenterException):
    """Base exception for all authentication/authorization errors."""


class UserNotFound(AuthError):
    """Raised when username is not found in the backend."""