from types import FunctionType
from typing import Any, List

from commandcenter.caching.core.util import (
    get_cached_func_name,
    get_fqn_type,
    get_return_value_type
)

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


class CachingException(CommandCenterException):
    """Base obj cache exception that all exceptions raised by cache decorators
    derive from.
    """


class UnhashableTypeError(CachingException):
    """Internal exception raised when a function argument is not hashable."""


class CacheKeyNotFoundError(CachingException):
    """The hash result of the inputs does not match the key in a cache result.
    
    This normally leads to a cache miss and is not propagated.
    """


class CacheError(CachingException):
    """Raised by the `memo` decorator when an object cannot be pickled or `memo`
    cannot read/write from/to the persisting backend.
    """


class UnhashableParamError(CachingException):
    """Raised when an input value to a caching function is not hashable.
    
    This can be avoided by attaching a leading underscore (_) to the argument.
    The argument will be skipped when calculating the cache key.
    """
    def __init__(
        self,
        func: FunctionType,
        arg_name: str | None,
        arg_value: Any,
        orig_exc: BaseException,
    ):
        msg = self._create_message(func, arg_name, arg_value)
        super().__init__(msg)
        self.with_traceback(orig_exc.__traceback__)

    @staticmethod
    def _create_message(
        func: FunctionType,
        arg_name: str | None,
        arg_value: Any,
    ) -> str:
        arg_name_str = arg_name if arg_name is not None else "(unnamed)"
        arg_type = get_fqn_type(arg_value)
        func_name = func.__name__
        arg_replacement_name = f"_{arg_name}" if arg_name is not None else "_arg"

        return " ".join(
            [
                segment.strip() for segment in
                """Cannot hash argument '{}' (of type {}) in '{}'. To address this,
                you can force this argument to be ignored by adding a leading
                underscore to the arguments name in the function signature (eg. '{}').
                """.format(
                    arg_name_str,
                    arg_type,
                    func_name,
                    arg_replacement_name
                ).splitlines()
            ]
        )


class UnserializableReturnValueError(CachingException):
    """Raised when a return value from a function cannot be serialized with pickle."""
    def __init__(self, func: FunctionType, return_value: FunctionType):
        msg = self._create_message(func, return_value)
        super().__init__(msg)
    
    def _create_message(self, func, return_value) -> str:
        return " ".join(
            [
                segment.strip() for segment in
                """Cannot serialize the return value of type '{}' in '{}'. 'memo'
                uses pickle to serialize the functions return value and safely store it in
                cache without mutating the original object. Please convert the return value
                to a pickle-serializable type. If you want to cache unserializable objects
                such as database connections or HTTP sessions, use 'singleton' instead.
                """.format(
                    get_return_value_type(return_value),
                    get_cached_func_name(func)
                ).splitlines()
            ]
        )


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