from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Union

import orjson
from pendulum.datetime import DateTime
from pydantic import BaseModel, validator

from commandcenter.core.integrations.models import BaseSubscription
from commandcenter.core.sources import AvailableSources
from commandcenter.core.integrations.util.common import (
    in_timezone,
    isoparse,
    json_loads
)



class SensorTypes(str, Enum):
    """Available sensory types in Traxx."""
    PROXIMITY = "proximity"
    VOLTS = "volts"
    TEMP = "temp"
    CURRENT = "current"
    AMBIENT = "ambient"


class TraxxSubscription(BaseSubscription):
    """Model for all Traxx subscriptions."""
    asset_id: int
    sensor_id: str
    sensor_type: SensorTypes = SensorTypes.PROXIMITY
    source: AvailableSources = AvailableSources.TRAXX

    def key(self) -> str:
        """Unique id for subscription."""
        return f"{self.sensor_id}-{self.asset_id}"


class BaseTraxxSensorMessage(BaseModel):
    """Base message for data returned from Traxx."""
    class Config:
        extra="ignore"
        allow_arbitrary_types=True
        json_dumps=lambda _obj, default: orjson.dumps(_obj, default=default).decode()
        json_loads = json_loads


class TraxxSensorItem(BaseTraxxSensorMessage):
    """Model for a single sample from Traxx."""
    timestamp: DateTime
    value: float

    @validator('timestamp', pre=True)
    def _parse_timestamp(cls, v: str) -> DateTime:
        """Parse timestamp (str) to DateTime."""
        if not isinstance(v, str):
            raise TypeError("Expected type str")
        try:
            return isoparse(v.split("+")[0].strip())
        except Exception as err:
            raise ValueError("Cannot parse timestamp") from err

    def in_timezone(self, timezone: str) -> None:
        """Convert sub item timestamp to specified timezone."""
        self.timestamp = in_timezone(self.timestamp, timezone)

    def json(self, *args, **kwargs) -> str:
        return orjson.dumps(self.dict(*args, **kwargs)).decode()

    def dict(self, *args, **kwargs) -> Dict[str, Union[str, Any]]:
        return {"timestamp": self.timestamp.isoformat(), "value": self.value}

    def __gt__(self, __o: object) -> bool:
        if not isinstance(__o, (datetime, TraxxSensorItem)):
            raise TypeError(f"'>' not supported between instances of {type(self)} and {type(__o)}")
        if isinstance(__o, TraxxSensorItem):
            return self.timestamp > __o.timestamp 
        else:
            return self.timestamp > __o

    def __ge__(self, __o: object) -> bool:
        if not isinstance(__o, (datetime, TraxxSensorItem)):
            raise TypeError(f"'>=' not supported between instances of {type(self)} and {type(__o)}")
        if isinstance(__o, TraxxSensorItem):
            return self.timestamp >= __o.timestamp 
        else:
            return self.timestamp >= __o

    def __lt__(self, __o: object) -> bool:
        if not isinstance(__o, (datetime, TraxxSensorItem)):
            raise TypeError(f"'<' not supported between instances of {type(self)} and {type(__o)}")
        if isinstance(__o, TraxxSensorItem):
            return self.timestamp < __o.timestamp 
        else:
            return self.timestamp < __o

    def __le__(self, __o: object) -> bool:
        if not isinstance(__o, (datetime, TraxxSensorItem)):
            raise TypeError(f"'<=' not supported between instances of {type(self)} and {type(__o)}")
        if isinstance(__o, TraxxSensorItem):
            return self.timestamp <= __o.timestamp 
        else:
            return self.timestamp <= __o


class TraxxSensorMessage(BaseTraxxSensorMessage):
    """Model for Traxx client message."""
    asset_id: int
    sensor_id: str
    items: List[TraxxSensorItem]

    def filter(self, last_timestamp: datetime) -> None:
        self.items = [item for item in self.items if item.timestamp > last_timestamp]

    def in_timezone(self, timezone: str) -> None:
        """Convert all timestamps to timezone unaware timestamps in specified
        timezone.
        """
        for item in self.items:
            item.in_timezone(timezone)


class BaseTraxxSubscriberMessage(BaseModel):
    """Base model for Traxx subscriber message."""
    class Config:
        extra = 'forbid'
        json_dumps=lambda _obj, default: orjson.dumps(_obj, default=default).decode()
        json_loads = json_loads


class TraxxSubscriberItem(BaseTraxxSubscriberMessage):
    """Model for a single sample from Traxx."""
    timestamp: str
    value: float


class TraxxSubscriberMessage(BaseTraxxSubscriberMessage):
    """Model for Traxx subscriber message."""
    asset_id: int
    sensor_id: str
    items: List[TraxxSubscriberItem]