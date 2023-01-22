"""Data integration tools for commandcenter.

Data integrations are the heart and soul of commandcenter. We employ a pub/sub
model for an event driven architecture. The core of data integrations lies in
the manager which acts as an exchange in a message broker setup. It connects
a client intefacing with a source to a subscriber consuming the data. The goal
of data integration is to make the experience consistent, reliable, and seemless
to the end user. An event coming from a source 'A' should have a comparable
structure to an event from source 'B'.

The model abstracts away the underlying protocol and data validation from the
consumer. It provides certain guarentees...
    1. Timeseries data is guarenteed to be in monotonically increasing order.
    2. Data is guarenteed to not be duplicated.
    3. Messages will contain data for a single subscription. No additional
    parsing is required to separate grouped data for multiple subscriptions.
"""
from .collections import AvailableTimeseriesCollections, LocalTimeseriesCollection
from .exceptions import (
    ClientSubscriptionError,
    FailedManager,
    IntegrationError,
    SubscriptionError,
    SubscriptionLimitError,
)
from .locks import AvailableLocks
from .managers import AvailableManagers, LocalManager
from .models import (
    BaseSubscription,
    ErrorMessage,
    BaseSubscriptionRequest,
)



__all__ = [
    "AvailableTimeseriesCollections",
    "LocalTimeseriesCollection",
    "ClientSubscriptionError",
    "FailedManager",
    "IntegrationError",
    "SubscriptionError",
    "SubscriptionLimitError",
    "AvailableLocks",
    "AvailableManagers",
    "LocalManager",
    "BaseSubscription",
    "ErrorMessage",
    "BaseSubscriptionRequest",
]