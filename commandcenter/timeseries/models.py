from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Dict, List

from pydantic import BaseModel

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
        return asdict(
            TimeseriesDocument(
                subscription=hash(self.subscription),
                timestamp=self.timestamp,
                value=self.value,
                expire=datetime.utcnow()
            )
        )


class TimeseriesSamples(BaseModel):
    """Data model for a collection of samples."""
    samples: List[TimeseriesSample]

    def __iter__(self) -> Iterable[Dict[str, Any]]:
        for sample in self.samples:
            yield sample.to_document()


class UnitOp(Tokenable):
    """Model for a unit operation. A unit op is a logical grouping of labeled
    subscriptions with associated meta data.
    """
    unitop_id: str
    data_mapping: Dict[str, AnySubscription]
    meta: Dict[str, Any] | None

    @property
    def token(self) -> ReferenceToken:
        return ReferenceToken(f"unitop-{self.unitop_id}")


class UnitOpQueryResult(BaseModel):
    """Response model for unit op queries."""
    items: List[UnitOp | None]