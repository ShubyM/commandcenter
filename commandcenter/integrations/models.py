import hashlib
from collections import OrderedDict
from typing import List, Sequence, Set

import orjson
from pydantic import BaseModel, validator

from commandcenter.util.serialization import json_loads
from commandcenter.sources import AvailableSources
from commandcenter.caching.tokens import ReferenceToken, Tokenable



class HashableModel(BaseModel):
    """A hashable base model.
    
    Models must json encode/decode(able). Hashes for use the JSON string
    representation of the object and are consistent across runtimes.

    Hashing: The `dict()` representation of the model is sorted alphanumerically
    by field name and then converted to JSON. The hashing algorithm used is
    SHAKE 128 with a 16 byte length. Finally, the hex digest is converted
    to an integer.
    """
    class Config:
        frozen=True
        json_dumps=lambda _obj, default: orjson.dumps(_obj, default=default).decode()
        json_loads=json_loads

    def __hash__(self) -> int:
        model = self.dict()
        sorted_ = OrderedDict(sorted_(model.items()))
        try:
            o = orjson.dumps(sorted_)
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


class AnySubscription(BaseSubscription):
    """A subscription model without a rigid structure."""
    class Config:
        extra="allow"


class ErrorMessage(HashableModel):
    """Standardized error message object."""
    exc: Exception
    subscriptions: Set[BaseSubscription]


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
        o = ''.join([str(hash(subscription)) for subscription in self.subscriptions]).encode()
        token = int(hashlib.shake_128(o).hexdigest(16), 16)
        return ReferenceToken(token=str(token))


class AnySubscriptionRequest(BaseSubscriptionRequest):
    """Subscription request model that allows for mixed subscriptions."""
    subscriptions: Sequence[AnySubscription]