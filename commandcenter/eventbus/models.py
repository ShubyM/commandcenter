from typing import Any, Dict

from jsonschema import SchemaError, ValidationError, validate
from jsonschema.validators import validator_for
from pydantic import Field, validator

from commandcenter.integrations.models import Subscription



class Topic(Subscription):
    name: str
    schema_: Dict[str, Any] = Field(alias="schema")
    
    @validator("schema_")
    def _is_valid_schema(cls, schema: Dict[str, Any]) -> Dict[str, Any]:
        validator_ = validator_for(schema)
        try:
            validator_.check_schema(schema)
        except SchemaError as e:
            raise ValueError(f"{e.json_path}-{e.message}")
        return schema


class Event(Subscription):
    topic: Topic
    payload: Dict[str, Any]

    @validator("payload")
    def _is_valid_payload(cls, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            validate(instance=payload, schema=cls.topic.schema_)
        except ValidationError as e:
            raise ValueError(f"{e.json_path}-{e.message}")
        return payload