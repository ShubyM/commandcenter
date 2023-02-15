import hashlib
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List

from pydantic import BaseModel, validator

from commandcenter.caching.tokens import ReferenceToken, Tokenable
from commandcenter.integrations.models import AnySubscription



@dataclass
class TimeseriesDocument:
    """Mongo document model for a timeseries sample."""
    subscription: int
    timestamp: datetime
    value: Any
    expire: datetime


class TimeseriesSample(BaseModel):
    """Data model for a timeseries sample."""
    subscription: AnySubscription
    timestamp: datetime
    value: Any

    def to_document(self) -> Dict[str, Any]:
        return TimeseriesDocument(
            subscription=hash(self.subscription),
            timestamp=self.timestamp,
            value=self.value,
            expire=datetime.utcnow()
        )


class TimeseriesSamples(BaseModel):
    """Data model for a collection of timeseries samples."""
    samples: Sequence[TimeseriesSample]

    def __iter__(self) -> Iterable[TimeseriesDocument]:
        for sample in self.samples:
            yield sample.to_document()


class UnitOp(Tokenable):
    """Model for a unit operation. A unit op is a logical grouping of labeled
    subscriptions with associated meta data.
    """
    unitop_id: str
    data_mapping: Dict[str, AnySubscription]
    meta: Dict[str, Any] | None

    @validator
    def _validate_unitop_id(cls, v: str) -> str:
        if " " in v:
            raise ValueError("'unitop_id' cannot contain spaces.")
        if not v.isascii():
            raise ValueError("'unitop_id' can only contain ASCII characters.")
        return v

    @property
    def token(self) -> ReferenceToken:
        token = int(hashlib.shake_128(f"unitop-{self.unitop_id}").hexdigest(16), 16)
        return ReferenceToken(token=token)
    
    @classmethod
    def get_token(cls, unitop_id: str) -> ReferenceToken:
        token = int(hashlib.shake_128(f"unitop-{unitop_id}").hexdigest(16), 16)
        return ReferenceToken(token=token)


class UnitOpQueryResult(BaseModel):
    """Response model for unit op queries."""
    items: List[UnitOp | None]