from commandcenter.exceptions import CommandCenterException



class EventBusError(CommandCenterException):
    """Base exception for all event bus errors."""


class EventBusClosed(EventBusError):
    """Raised when the bus is closed and cannot accept subscriptions."""


class EventBusSubscriptionError(EventBusError):
    """Base event for subscription related errors."""


class EventBusSubscriptionLimitError(EventBusSubscriptionError):
    """Raised when the maximum subscribers exist on the bus."""
    def __init__(self, max_subscribers: int) -> None:
        self.max_subscribers = max_subscribers

    def __str__(self) -> str:
        return "Subscription limit reached ({}).".format(self.max_subscribers)


class EventBusSubscriptionTimeout(EventBusSubscriptionError):
    """Raised when the timeout limit to subscribe is reached."""


class EventBusConnectionError(EventBusSubscriptionError):
    """Raised if the broker connection closed before we could subscribe."""