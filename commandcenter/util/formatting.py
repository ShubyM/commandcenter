import re
import json
from typing import List

import orjson

from commandcenter.types import JSONPrimitive, TimeseriesRow



_CAPITALIZE_PATTERN = re.compile('(?<!^)(?=[A-Z])')


def snake_to_camel(string: str) -> str:
    """Covert snake case (arg_a) to camel case (ArgA)."""
    return ''.join(word.capitalize() for word in string.split('_'))


def snake_to_lower_camel(string: str) -> str:
    """Covert snake case (arg_a) to lower camel case (argA)."""
    splits = string.split('_')
    if len(splits) == 1:
        return string
    return f"{splits[0]}{''.join(word.capitalize() for word in splits[1:])}"


def camel_to_snake(string: str) -> str:
    """Convert camel (ArgA) and lower camel (argB) to snake case (arg_a)"""
    return _CAPITALIZE_PATTERN.sub('_', string).lower()


def json_loads(v: str | bytes):
    """JSON decoder which uses orjson for bytes and builtin json for str."""
    match v:
        case str():
            return json.loads(v)
        case bytes():
            return orjson.loads(v)
        case _:
            raise TypeError(f"Expected str | bytes, got {type(v)}")


def format_timeseries_rows(row: TimeseriesRow) -> List[JSONPrimitive]:
    """Formats a timeseries row as an iterable which can be converted to a row
    for a file format.
    """
    return [row[0].isoformat(), *row[1]]