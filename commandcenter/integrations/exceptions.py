from typing import List

from commandcenter.exceptions import CommandCenterException



class IntegrationError(CommandCenterException):
    """Base exception for all data integration errors."""


class SubscriptionError(IntegrationError):
    """Raised when attempting to subscribe to a manager and the client was
    unable to subscribe to one or more subscriptions.
    """


class SubscriptionLimitError(SubscriptionError):
    """Raised when attempting to subscribe to a manager with the maximum number
    of active subscribers.
    """
    def __init__(self, max_subscribers: int) -> None:
        self.max_subscribers = max_subscribers

    def __str__(self) -> str:
        return "Subscription limit reached ({}).".format(self.max_subscribers)


class ClientSubscriptionError(SubscriptionError):
    """Raised when attempting to subscribe to a manager and the client was
    unable to subscribe to one or more subscriptions due to an unhandled exception.
    """
    def __init__(self, error: BaseException) -> None:
        self.error = error

    def __str__(self) -> str:
        return "Unable to subscribe due to an unhandled exception in the client."


class ManagerClosed(IntegrationError):
    """Raised when attempting to subscribe to a manager instance is closed."""
    def __init__(self, errors: List[BaseException]) -> None:
        self.errors = errors

    def __str__(self) -> str:
        return "Unable to subscribe due to {} error(s) in manager.".format(len(self.exc))


class ClientClosed(IntegrationError):
    """Raised when a method is called on a closed client."""


class DroppedSubscriber(IntegrationError):
    """Can be raised when a subscriber has been stopped on the manager side."""