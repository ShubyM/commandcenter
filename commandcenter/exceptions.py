from typing import List

class CommandCenterException(Exception):
    """Base exception for all commandcenter errors."""


class NotConfigured(CommandCenterException):
    """Raised when a feature is not enabled, usually due to a missing requirment
    in the config.
    """


class SubProcessError(CommandCenterException):
    """Base exception for subprocess errors."""


class NonZeroExitCode(SubProcessError):
    """Raised when a process exits with a non-zero exit code."""
    def __init__(self, code: int) -> None:
        self.code = code

    def __str__(self) -> str:
        return "Process exited with non-zero exit code ({}).".format(self.code)


class AuthError(CommandCenterException):
    """Base exception for all auth errors."""


class UserNotFound(AuthError):
    """Raised when a user is not found in the backend database."""
    def __init__(self, username: str) -> None:
        self.username = username

    def __str__(self) -> str:
        return "{} not found.".format(self.username)


class ActiveDirectoryError(AuthError):
    """Base exception for AD related errors."""


class NoHostsFound(ActiveDirectoryError):
    """Raised when no domain server hosts are found for a given domain name."""
    def __init__(self, domain: str) -> None:
        self.domain = domain

    def __str__(self) -> str:
        return "No hosts found for {}.".format(self.domain)


class IntegrationError(CommandCenterException):
    """Base exception for all data integration errors."""


class SubscriptionError(IntegrationError):
    """Raised when attempting to subscribe to a manager and the client was
    unable to subscribe to the subscriptions.
    """


class SubscriptionLimitError(SubscriptionError):
    """Raised when attempting to subscribe to a manager or client with the
    maximum number of active subscribers.
    """
    def __init__(self, max_subscribers: int) -> None:
        self.max_subscribers = max_subscribers

    def __str__(self) -> str:
        return "Subscription limit reached ({}).".format(self.max_subscribers)


class ClientError(IntegrationError):
    """Raised when attempting to subscribe to a manager and the client was
    unable to subscribe to one or more subscriptions due to an unhandled exception.
    """
    def __init__(self, error: BaseException) -> None:
        self.error = error

    def __str__(self) -> str:
        return "Unable to subscribe due to an unhandled exception in the client."


class ManagerClosed(IntegrationError):
    """Raised when attempting to subscribe to a manager instance is closed."""


class ClientClosed(IntegrationError):
    """Raised when a method is called on a closed client."""


class DroppedSubscriber(IntegrationError):
    """Can be raised when a subscriber has been stopped on the manager side."""


class PIWebIntegrationError(IntegrationError):
    """Base exception for pi_web integration errors."""


class PIWebContentError(PIWebIntegrationError):
    """Raised when a required property is not present in the body of a successful
    response.
    """


class PIWebResponseError(PIWebIntegrationError):
    """Raised when the `Errors` property is present in the body of successful
    HTTP response.
    """
    def __init__(self, errors: List[str]) -> None:
        self.errors = errors

    def __str__(self) -> str:
        return "{} errors returned in response body.".format(len(self.errors))


class TraxxIntegrationError(IntegrationError):
    """Base exception for Traxx errors."""


class TraxxExpiredSession(TraxxIntegrationError):
    """Raised when the session cookie is expired and we can no longer authenticate
    with the server.
    """

    def __str__(self) -> str:
        return "Signed out. Please refresh session config"