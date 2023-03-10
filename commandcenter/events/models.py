from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Tuple

import orjson
from jsonschema import SchemaError
from jsonschema.validators import validator_for
from pydantic import BaseModel, Field, root_validator, validator

from commandcenter.caching.tokens import ReferenceToken, Tokenable
from commandcenter.integrations.models import BaseSubscriptionRequest, Subscription



class Topic(Tokenable):
    """A subscription model tied to a class of events."""
    topic: str
    schema_: Dict[str, Any] = Field(alias="schema")
    
    @validator("schema_")
    def _is_valid_schema(cls, schema: Dict[str, Any]) -> Dict[str, Any]:
        validator_ = validator_for(schema)
        try:
            validator_.check_schema(schema)
        except SchemaError as e:
            raise ValueError(f"{e.json_path}-{e.message}")
        return schema
    
    @property
    def token(self) -> ReferenceToken:
        return ReferenceToken(f"topic-{self.topic}")


class TopicQueryResult(BaseModel):
    """Result set of topic query."""
    items: List[str | None]


class TopicSubscription(Subscription):
    """Subscription model for a topic or subset of a topic."""
    topic: str
    routing_key: str | None
    
    @root_validator
    def _set_routing_key(cls, v: Dict[str, str | None]) -> Dict[str, str]:
        topic = v.get("topic")
        routing_key = v.get("routing_key")
        if routing_key and routing_key.startswith(topic):
            return v
        if routing_key:
            routing_key = ".".join([split for split in routing_key.split(".") if split])
            v["routing_key"] = f"{topic}.{routing_key}"
        else:
            v["routing_key"] = f"{topic}.#"
        return v


class TopicSubscriptionRequest(BaseSubscriptionRequest):
    subscriptions: Sequence[TopicSubscription]


class Event(BaseModel):
    """An event to publish."""
    topic: str
    routing_key: str | None
    payload: Dict[str, Any]

    @root_validator
    def _prep_routing_key(cls, v: Dict[str, str | None]) -> Dict[str, str]:
        topic = v.get("topic")
        routing_key = v.get("routing_key")
        if routing_key and routing_key.startswith(topic):
            return v
        if routing_key:
            routing_key = ".".join([split for split in routing_key.split(".") if split])
            v["routing_key"] = f"{topic}.{routing_key}"
        else:
            v["routing_key"] = f"{topic}"
        return v
    
    def publish(self) -> Tuple[str, bytes]:
        return self.routing_key, orjson.dumps(self.payload)

    def to_document(self) -> Dict[str, Any]:
        return asdict(
            EventDocument(
                topic=self.topic,
                routing_key=self.routing_key,
                payload=self.payload,
                timestamp=datetime.utcnow()
            )
        )


class EventQueryResult(BaseModel):
    """Result set of event query."""
    items: List[Event | None]
    

@dataclass
class EventDocument:
    topic: str
    routing_key: str
    payload: Dict[str, Any]
    timestamp: datetime


class EventSubscriberStatus(str, Enum):
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"


class EventSubscriberInfo(BaseModel):
    """Model for subscriber statistics."""
    name: str
    stopped: bool
    status: EventSubscriberStatus
    created: datetime
    uptime: int
    total_published_events: int
    total_subscriptions: int