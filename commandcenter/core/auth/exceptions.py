from typing import Set

from commandcenter.exceptions import CommandCenterException



class AuthError(CommandCenterException):
    """Base exception for all authentication/authorization errors."""


class UserNotFound(AuthError):
    """Raised when a user is not found in the backend databse."""
    def __init__(self, username: str) -> None:
        self.username = username

    def __str__(self) -> str:
        return "{} not found.".format(self.username)