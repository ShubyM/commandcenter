from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

import orjson
from pendulum.datetime import DateTime
from pydantic import BaseModel, root_validator, validator

from commandcenter.core.integrations.models import BaseSubscription
from commandcenter.core.sources import AvailableSources
from commandcenter.core.integrations.util.common import (
    in_timezone,
    isoparse,
    json_loads,
    to_camel
)



def restrict_obj_type(v: str) -> str:
    if v != PIObjType.POINT:
        raise ValueError("Only obj_type 'point' is currently supported")
    return v


class WebIdType(str, Enum):
    FULL = "Full"
    IDONLY = "IDOnly"
    PATHONLY = "PathOnly"
    LOCALIDONLY = "LocalIDOnly"
    DEFAULTIDONLY = "DefaultIDOnly"


class PIObjType(str, Enum):
    POINT = "point"
    ATTRIBUTE = "attribute"
    ELEMENT = "element"


class PIObjSearch(BaseModel):
    search: str
    web_id_type: WebIdType = WebIdType.FULL
    obj_type: PIObjType = PIObjType.POINT
    server: Optional[str] = None
    database: Optional[str] = None

    _obj_type = validator("obj_type", allow_reuse=True)(restrict_obj_type)
    
    def to_subscription(self, web_id: str, name: str) -> "PISubscription":
        return PISubscription(
            web_id=web_id,
            web_id_type=self.web_id_type,
            obj_type=self.obj_type,
            name=name
        )

class PISubscription(BaseSubscription):
    web_id: str
    name: Optional[str] = None
    web_id_type: WebIdType = WebIdType.FULL
    obj_type: PIObjType = PIObjType.POINT
    source: AvailableSources = AvailableSources.PI_WEB_API
    
    _obj_type = validator("obj_type", allow_reuse=True)(restrict_obj_type)

    @validator("source")
    def _restrict_source(cls, v: str) -> str:
        if v != AvailableSources.PI_WEB_API:
            raise ValueError(f"Invalid source '{v}' for {cls.__class__.__name__}")
        return v


class PIBaseChannelMessage(BaseModel):
    """
    Base message for data returned from 'streamsets/channels' endpoint.

    Sample Message (Raw)
    {
    "Links": {},
    "Items": [
        {
        "WebId": "F1DPEmoryo_bV0GzilxLXH31pgjowAAAQUJDX1BJX09QU1xBSUM2ODEwNTkuUFY",
        "Name": "AIC681059.PV",
        "Path": "\\\\ABC_PI_Ops\\AIC681059.PV",
        "Links": {},
        "Items": [
            {
            "Timestamp": "2022-12-09T14:06:36Z",
            "Value": 50.40764,
            "UnitsAbbreviation": "",
            "Good": true,
            "Questionable": false,
            "Substituted": false,
            "Annotated": false
            }
        ],
        "UnitsAbbreviation": ""
        },
        {
        "WebId": "F1DPEmoryo_bV0GzilxLXH31pgcAQAAAQUJDX1BJX09QU1xGSVExNDAxMi5QVg",
        "Name": "FIQ14012.PV",
        "Path": "\\\\ABC_PI_Ops\\FIQ14012.PV",
        "Links": {},
        "Items": [
            {
            "Timestamp": "2022-12-09T14:06:37Z",
            "Value": 65229868.0,
            "UnitsAbbreviation": "",
            "Good": true,
            "Questionable": false,
            "Substituted": false,
            "Annotated": false
            }
        ],
        "UnitsAbbreviation": ""
        },
        {
        "WebId": "F1DPEmoryo_bV0GzilxLXH31pgLAcAAAQUJDX1BJX09QU1xUSTE0MDEzLlBW",
        "Name": "TI14013.PV",
        "Path": "\\\\ABC_PI_Ops\\TI14013.PV",
        "Links": {},
        "Items": [
            {
            "Timestamp": "2022-12-09T14:06:38.4350128Z",
            "Value": 73.68963,
            "UnitsAbbreviation": "",
            "Good": true,
            "Questionable": false,
            "Substituted": false,
            "Annotated": false
            }
        ],
        "UnitsAbbreviation": ""
        }
    ]
    }

    Target conversion to
    [
        {
            'name': 'AIC681059.PV',
            'web_id': 'F1DPEmoryo_bV0GzilxLXH31pgjowAAAQUJDX1BJX09QU1xBSUM2ODEwNTkuUFY',
            'items': [
                {'timestamp': '2022-12-09T09:06:36', 'value': 50.40764}
            ]
        },
        {
            'name': 'FIQ14012.PV',
            'web_id': 'F1DPEmoryo_bV0GzilxLXH31pgcAQAAAQUJDX1BJX09QU1xGSVExNDAxMi5QVg',
            'items': [
                {'timestamp': '2022-12-09T09:06:37', 'value': 65229868.0}
            ]
        },
        {
            'name': 'TI14013.PV',
            'web_id': 'F1DPEmoryo_bV0GzilxLXH31pgLAcAAAQUJDX1BJX09QU1xUSTE0MDEzLlBW',
            'items': [
                {'timestamp': '2022-12-09T09:06:38.435012', 'value': 73.68963}
            ]
        }
    ]
    """
    class Config:
        alias_generator = to_camel
        extra = 'ignore'
        json_dumps=lambda _obj, default: orjson.dumps(_obj, default=default).decode()
        json_loads = json_loads
        allow_arbitrary_types=True


class PIMessageSubItem(PIBaseChannelMessage):
    """Model for sub items containing timeseries data for a particular WebId."""
    timestamp: DateTime
    value: Any
    good: bool

    @validator('timestamp', pre=True)
    def _parse_timestamp(cls, v: str) -> DateTime:
        """Parse timestamp (str) to DateTime."""
        if not isinstance(v, str):
            raise TypeError("Expected type str")
        try:
            return isoparse(v)
        except Exception as err:
            raise ValueError("Cannot parse timestamp") from err

    @root_validator
    def _format_content(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        """Replace value of not 'good' items with `None`."""
        value, good = v.get("value"), v.get("good")
        if not good:
            v["value"] = None
        elif value is not None:
            if isinstance(value, dict):
                v["value"] = value["Name"]
        
        return v
    
    def in_timezone(self, timezone: str) -> None:
        """Convert sub item timestamp to specified timezone."""
        self.timestamp = in_timezone(self.timestamp, timezone)

    def json(self, *args, **kwargs) -> str:
        return orjson.dumps(self.dict(*args, **kwargs)).decode()

    def dict(self, *args, **kwargs) -> Dict[str, Union[str, Any]]:
        return {"timestamp": self.timestamp.isoformat(), "value": self.value}

    def __gt__(self, __o: object) -> bool:
        if not isinstance(__o, (datetime, PIMessageSubItem)):
            raise TypeError(f"'>' not supported between instances of {type(self)} and {type(__o)}")
        if isinstance(__o, PIMessageSubItem):
            return self.timestamp > __o.timestamp 
        else:
            return self.timestamp > __o

    def __ge__(self, __o: object) -> bool:
        if not isinstance(__o, (datetime, PIMessageSubItem)):
            raise TypeError(f"'>=' not supported between instances of {type(self)} and {type(__o)}")
        if isinstance(__o, PIMessageSubItem):
            return self.timestamp >= __o.timestamp 
        else:
            return self.timestamp >= __o

    def __lt__(self, __o: object) -> bool:
        if not isinstance(__o, (datetime, PIMessageSubItem)):
            raise TypeError(f"'<' not supported between instances of {type(self)} and {type(__o)}")
        if isinstance(__o, PIMessageSubItem):
            return self.timestamp < __o.timestamp 
        else:
            return self.timestamp < __o

    def __le__(self, __o: object) -> bool:
        if not isinstance(__o, (datetime, PIMessageSubItem)):
            raise TypeError(f"'<=' not supported between instances of {type(self)} and {type(__o)}")
        if isinstance(__o, PIMessageSubItem):
            return self.timestamp <= __o.timestamp 
        else:
            return self.timestamp <= __o


class PIMessageItem(PIBaseChannelMessage):
    """Model for single top level item pertaining to a WebId."""
    name: str
    web_id: str
    items: List[PIMessageSubItem]

    @validator("items")
    def _sort_items(cls, v: List[PIMessageSubItem]) -> List[PIMessageSubItem]:
        # Timeseries values for a given WebId are not guarenteed to be in
        # chronological order so we sort the items on the timestamp to ensure
        # they are
        # https://docs.osisoft.com/bundle/pi-web-api-reference/page/help/topics/channels.html
        return sorted(v)


class PIMessage(PIBaseChannelMessage):
    """Model for streamset messages received from PI Web API."""
    items: List[PIMessageItem]

    def in_timezone(self, timezone: str) -> None:
        """Convert all timestamps to timezone unaware timestamps in specified
        timezone.
        """
        for item in self.items:
            for sub_item in item.items:
                sub_item.in_timezone(timezone)


class PIBaseSubscriberMessage(BaseModel):
    class Config:
        extra = 'forbid'
        json_dumps=lambda _obj, default: orjson.dumps(_obj, default=default).decode()
        json_loads = json_loads
    

class PISubscriberSubItem(PIBaseSubscriberMessage):
    """Model for sub items containing timeseries data for a particular WebId."""
    timestamp: str
    value: Any


class PISubscriberItem(PIBaseSubscriberMessage):
    """Model for single top level item pertaining to a WebId."""
    name: str
    web_id: str
    items: List[PISubscriberSubItem]


class PISubscriberMessage(PIBaseSubscriberMessage):
    """Model for PI messages from a subscriber."""
    items: List[PISubscriberItem]