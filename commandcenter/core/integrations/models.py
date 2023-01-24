import hashlib
from typing import List, Sequence, Set

import orjson
from attrs import frozen, field
from attrs.validators import deep_iterable, instance_of
from pydantic import BaseModel, validator

from commandcenter.core.integrations.util.common import json_loads
from commandcenter.core.sources import AvailableSources
from commandcenter.core.util.cache import AbstractTokenGenerator, ReferenceToken



class HashableModel(BaseModel):
    """A hashable base model.
    
    Models must be hashable and json encode/decode(able). Hashes for use the
    JSON string representation of the object and are consistent across runtimes.
    """
    class Config:
        frozen=True
        json_dumps=lambda _obj, default: orjson.dumps(_obj, default=default).decode()
        json_loads=json_loads

    def __hash__(self) -> int:
        try:
            o = self.json().encode()
        except Exception as e:
            raise TypeError(f"unhashable type: {e.__str__()}")
        return int(hashlib.shake_128(o).hexdigest(16), 16)

class BaseSubscription(HashableModel):
    """Base model for all subscriptions.

    Note: Subscription implementations must not override the comparison operators.
    These operators are based on the hash of the model which is critical when
    sorting sequences of mixed subscription types.
    """
    source: AvailableSources
    
    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, BaseSubscription):
            return False
        try:
            return hash(self) == hash(__o)
        except TypeError:
            return False
    
    def __gt__(self, __o: object) -> bool:
        if not isinstance(__o, BaseSubscription):
            raise TypeError(f"'>' not supported between instances of {type(self)} and {type(__o)}.")
        try:
            return hash(self) > hash(__o)
        except TypeError:
            return False
    
    def __lt__(self, __o: object) -> bool:
        if not isinstance(__o, BaseSubscription):
            raise TypeError(f"'<' not supported between instances of {type(self)} and {type(__o)}.")
        try:
            return hash(self) < hash(__o)
        except TypeError:
            return False


@frozen
class ErrorMessage:
    """Standardized error message object which is handled by an `AbstractManager`.
    
    If an error occurs that causes a connection to close in an `AbstractClient`
    it must produce an error message.
    Args:
        exc: The exception that occurred
        subscriptions: The subscriptions the connection managed
    Raises:
        TypeError: If an invalid type is passed to the constructor
    """
    exc: Exception = field(validator=[instance_of(Exception)])
    subscriptions: Set["BaseSubscription"] = field(
        validator=[deep_iterable(instance_of(BaseSubscription), instance_of(set))]
    )
    def __repr__(self) -> str:
        return "{} - {} subscriptions".format(self.exc.__class__.__name__, len(self.subscriptions))


class BaseSubscriptionRequest(AbstractTokenGenerator):
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
        o = ''.join([str(hash(subscription)) for subscription in self.subscriptions]).encode()
        token = int(hashlib.shake_128(o).hexdigest(16), 16)
        return ReferenceToken(token=str(token))