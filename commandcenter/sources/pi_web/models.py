import hashlib
import itertools
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Union

import orjson
from pendulum.datetime import DateTime
from pydantic import BaseModel, root_validator, validator

from commandcenter.caching.tokens import ReferenceToken, Tokenable
from commandcenter.integrations.models import (
    BaseSubscription,
    BaseSubscriptionRequest,
    HashableModel
)
from commandcenter.sources import Sources
from commandcenter.util import (
    in_timezone,
    isoparse,
    json_loads,
    snake_to_camel
)


# TODO: Expand to AF elements and attributes
def restrict_obj_type(v: str) -> str:
    if v != PIObjType.POINT:
        raise ValueError("Only obj_type 'point' is currently supported")
    return v


class WebIdType(str, Enum):
    """https://docs.aveva.com/bundle/pi-web-api-reference/page/help/topics/webid-type.html"""
    FULL = "Full"
    IDONLY = "IDOnly"
    PATHONLY = "PathOnly"
    LOCALIDONLY = "LocalIDOnly"
    DEFAULTIDONLY = "DefaultIDOnly"


class PIObjType(str, Enum):
    POINT = "point"
    ATTRIBUTE = "attribute"
    ELEMENT = "element"


class PISubscription(BaseSubscription):
    """Model for all PI object subscriptions."""
    web_id: str
    name: str | None = None
    web_id_type: WebIdType = WebIdType.FULL
    obj_type: PIObjType = PIObjType.POINT
    source: Sources = Sources.PI_WEB_API
    
    validator("obj_type", allow_reuse=True)(restrict_obj_type)

    @validator("source")
    def _restrict_source(cls, v: str) -> str:
        if v != Sources.PI_WEB_API:
            raise ValueError(f"Invalid source '{v}' for {cls.__class__.__name__}")
        return v


class PISubscriptionRequest(BaseSubscriptionRequest):
    """Model for PI Web subscription requests."""
    subscriptions: List[PISubscription]


class PIObjSearch(HashableModel):
    """Model for client to search for a WebId based on a search key."""
    search: str
    web_id_type: WebIdType = WebIdType.FULL
    obj_type: PIObjType = PIObjType.POINT
    server: str | None = None
    database: str | None = None

    validator("obj_type", allow_reuse=True)(restrict_obj_type)
    
    def to_subscription(self, web_id: str, name: str) -> PISubscription:
        return PISubscription(
            web_id=web_id,
            web_id_type=self.web_id_type,
            obj_type=self.obj_type,
            name=name
        )

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, PIObjSearch):
            return False
        try:
            return hash(self) == hash(__o)
        except TypeError:
            return False
    
    def __gt__(self, __o: object) -> bool:
        if not isinstance(__o, PIObjSearch):
            raise TypeError(f"'>' not supported between instances of {type(self)} and {type(__o)}.")
        try:
            return hash(self) > hash(__o)
        except TypeError:
            return False
    
    def __lt__(self, __o: object) -> bool:
        if not isinstance(__o, PIObjSearch):
            raise TypeError(f"'<' not supported between instances of {type(self)} and {type(__o)}.")
        try:
            return hash(self) < hash(__o)
        except TypeError:
            return False


class PIObjSearchFailed(HashableModel):
    """Model to detail why an object search failed."""
    obj: PIObjSearch
    reason: str


class PIObjSearchRequest(Tokenable):
    """Model for PI object search requests."""
    search: List[PIObjSearch]

    @validator("search")
    def _sort_search(cls, search: Sequence[PIObjSearch]) -> List[PIObjSearch]:
        search = list(set(search))
        return sorted(search)

    @property
    def token(self) -> ReferenceToken:
        """A unique iddentification for this sequence of searches. Tokens are
        stable across runtimes.
        """
        o = ''.join([str(hash(search)) for search in self.search]).encode()
        token = int(hashlib.shake_128(o).hexdigest(16), 16)
        return ReferenceToken(token=str(token))


class PIObjSearchResult(Tokenable):
    """The results of an object search. Searches which returned a result are
    converted to subscriptions. Failed searches provide a reason.
    """
    subscriptions: List[Optional[PISubscription]]
    failed: List[Optional[PIObjSearchFailed]]

    @property
    def token(self) -> ReferenceToken:
        """A unique iddentification for this sequence of searches. Tokens are
        stable across runtimes.
        """
        subscriptions = sorted(self.subscriptions)
        failed = sorted(self.failed)
        o = 'searchresult'.join([str(hash(obj)) for obj in itertools.chain(subscriptions, failed)]).encode()
        token = int(hashlib.shake_128(o).hexdigest(16), 16)
        return ReferenceToken(token=str(token))


class PIBaseChannelMessage(BaseModel):
    """
    Base message for data returned from 'streamsets/channels' endpoint.

    ```
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
    ```
    Target conversion to
    ```
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
    ```
    """
    class Config:
        alias_generator = snake_to_camel
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
    """Base model for PI subscriber messages."""
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

    def subscription_message(self, subscription: PISubscription) -> str:
        """JSON representation of subscription with items."""
        return orjson.dumps(
            {
                "subscription": subscription.dict(),
                "items": self.dict()["items"]
            }
        ).decode()


class PISubscriberMessage(PIBaseSubscriberMessage):
    """Model for PI subscriber messages."""
    items: List[PISubscriberItem]