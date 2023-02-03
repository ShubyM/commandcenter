import hashlib
from datetime import datetime
from enum import IntEnum
from typing import Any, Dict, List, Sequence, Set

import orjson
from pydantic import BaseModel, validator

from commandcenter.caching.tokens import ReferenceToken, Tokenable
from commandcenter.sources import Sources
from commandcenter.util import json_loads



class Subscription(BaseModel):
    """A hashable base model.
    
    Models must be json encode/decode(able). Hashes use the JSON string
    representation of the object and are consistent across runtimes.

    Hashing: The `dict()` representation of the model is converted to a JSON
    byte string which is then sorted. The hashing algorithm used is SHAKE 128
    with a 16 byte length. Finally, the hex digest is converted to a base 10
    integer.

    Note: Implementations must not override the comparison operators.
    These operators are based on the hash of the model which is critical when
    sorting sequences of mixed implementation types.
    """
    class Config:
        frozen=True
        json_dumps=lambda _obj, default: orjson.dumps(_obj, default=default).decode()
        json_loads=json_loads

    def __hash__(self) -> int:
        try:
            o = bytes(sorted(orjson.dumps(self.dict())))
        except Exception as e:
            raise TypeError(f"unhashable type: {e.__str__()}")
        return int(hashlib.shake_128(o).hexdigest(16), 16)

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, Subscription):
            return False
        try:
            return hash(self) == hash(__o)
        except TypeError:
            return False
    
    def __gt__(self, __o: object) -> bool:
        if not isinstance(__o, Subscription):
            raise TypeError(f"'>' not supported between instances of {type(self)} and {type(__o)}.")
        try:
            return hash(self) > hash(__o)
        except TypeError:
            return False
    
    def __lt__(self, __o: object) -> bool:
        if not isinstance(__o, Subscription):
            raise TypeError(f"'<' not supported between instances of {type(self)} and {type(__o)}.")
        try:
            return hash(self) < hash(__o)
        except TypeError:
            return False


class BaseSubscription(Subscription):
    """Base model for all subscriptions."""
    source: Sources


class AnySubscription(BaseSubscription):
    """A subscription model without a rigid structure."""
    class Config:
        extra="allow"


class DroppedSubscriptions(BaseModel):
    """Message for dropped subscriptions from a client to a manager."""
    subscriptions: Set[Subscription | None]
    error: Exception | None

    class Config:
        arbitrary_types_allowed=True

    @validator("error")
    def _is_exception(cls, v: Exception) -> Exception:
        if v and not isinstance(v, Exception):
            raise TypeError(f"Expected 'Exception', got {type(v)}")
        return v


class BaseSubscriptionRequest(Tokenable):
    """Base model for a sequence of subscriptions that a client registers with
    one or more integrations.
    """
    subscriptions: Sequence[BaseSubscription]
    
    @validator("subscriptions")
    def _sort_subscriptions(cls, subscriptions: Sequence[BaseSubscription]) -> List[BaseSubscription]:
        subscriptions = list(set(subscriptions))
        return sorted(subscriptions)

    @property
    def token(self) -> ReferenceToken:
        """A unique iddentification for this sequence of subscriptions. Tokens are
        stable across runtimes.
        """
        o = f'{self.__class__.__name__}'.join(
            [str(hash(subscription)) for subscription in self.subscriptions]
        ).encode()
        token = int(hashlib.shake_128(o).hexdigest(16), 16)
        return ReferenceToken(token=str(token))


class AnySubscriptionRequest(BaseSubscriptionRequest):
    """Subscription request model that allows for mixed subscriptions."""
    subscriptions: Sequence[AnySubscription]

    def group(self) -> Dict[Sources, List[AnySubscription]]:
        sources = set([subscription.source for subscription in self.subscriptions])
        groups = {}
        for source in sources:
            group = [
                subscription for subscription in self.subscriptions
                if subscription.source == source
            ]
            groups[source] = group
        return groups


class AnySubscriberMessage(BaseModel):
    """Unconstrained subscriber message. Used primarily for OpenAPI schema"""
    subscription: BaseSubscription
    items: List[Any]


class SubscriberCodes(IntEnum):
    """Codes returned from a `wait` on a subscriber."""
    STOPPED = 1
    DATA = 2


class ConnectionInfo(BaseModel):
    """Model for connection statistics."""
    name: str
    online: bool
    created: datetime
    uptime: int
    total_published_messages: int
    total_subscriptions: int


class ClientInfo(BaseModel):
    """Model for client statistics."""
    name: str
    closed: bool
    data_queue_size: int
    dropped_connection_queue_size: int
    created: datetime
    uptime: int
    active_connections: int
    active_subscriptions: int
    subscription_capacity: int
    total_connections_serviced: int
    connection_info: List[Dict[str, str | ConnectionInfo]]


class SubscriberInfo(BaseModel):
    """Model for subscriber statistics."""
    name: str
    stopped: bool
    created: datetime
    uptime: int
    total_published_messages: int
    total_subscriptions: int


class ManagerInfo(BaseModel):
    """Model for manager statistics."""
    name: str
    closed: bool
    created: datetime
    uptime: int
    active_subscribers: int
    active_subscriptions: int
    subscriber_capacity: int
    total_subscribers_serviced: int
    client_info: ClientInfo
    subscriber_info: List[Dict[str, str | SubscriberInfo]]