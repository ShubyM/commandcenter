from commandcenter.exceptions import CommandCenterException



class EventError(CommandCenterException):
    """Base exception for all event bus errors."""


class EventBusClosed(EventError):
    """Raised when the bus is closed and cannot accept subscriptions."""


class EventSubscriptionError(EventError):
    """Base event for subscription related errors."""


class EventSubscriptionLimitError(EventSubscriptionError):
    """Raised when the maximum subscribers exist on the bus."""
    def __init__(self, max_subscribers: int) -> None:
        self.max_subscribers = max_subscribers

    def __str__(self) -> str:
        return "Subscription limit reached ({}).".format(self.max_subscribers)


class EventSubscriptionTimeout(EventSubscriptionError):
    """Raised when the timeout limit to subscribe is reached."""