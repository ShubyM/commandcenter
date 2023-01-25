import json
import logging
import random
import re
import subprocess
from collections.abc import Iterable
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Tuple, Type

import dateutil.parser
import orjson
import pendulum
from pendulum.datetime import DateTime

from commandcenter.exceptions import NonZeroExitCode
from commandcenter.types import TimeseriesRow


TIMEZONE = pendulum.now().timezone_name

capitalize_pattern = re.compile('(?<!^)(?=[A-Z])')


class ObjSelection(str, Enum):
    """Define a selection of objects.
    
    Examples:
    >>> class AuthBackends(ObjSelection):
    ...     DEFAULT = "default", ActiveDirectoryBackend
    ...     ACTIVE_DIRECTORY = "activedirectory", ActiveDirectoryBackend

    Now we can select a backend by a key
    >>> backend = AuthBackends("activedirectory").cls
    
    This is particularly useful in confguration
    >>> config = Config(".env")
    >>> BACKEND = config(
    ...     "BACKEND",
    ...     cast=lambda v: AuthBackends(v).cls,
    ...     default=AuthBackends.default.value
    ... )
    """
    def __new__(cls, value: str, type_: Type[Any]) -> "ObjSelection":
        obj = str.__new__(cls, value)
        obj._value_ = value
        
        obj.cls = type_  # type: ignore[attr-defined]
        return obj


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
    return capitalize_pattern.sub('_', string).lower()


def run_subprocess(
    command: List[str],
    logger: logging.Logger,
    raise_non_zero: bool = True
) -> None:
    """Run a subprocess and route the `stdout` and `stderr` to the logger.
    
    Stdout is debug information and stderr is warning.

    Raises:
        NonZeroExitCode: Process exited with non-zero exit code.
    """
    with subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    ) as process:
        for line in process.stdout.readlines():
            logger.debug(line.decode().rstrip("\r\n"))
        for line in process.stderr.readlines():
            logger.warning(line.decode().rstrip("\r\n"))
    if process.returncode != 0:
        logger.warning("Process exited with non-zero exit code (%i)", process.returncode)
        if raise_non_zero:
            raise NonZeroExitCode(process.returncode)


def json_loads(v: str | bytes):
    """JSON decoder which uses orjson for bytes and builtin json for str."""
    match v:
        case str():
            return json.loads(v)
        case bytes():
            return orjson.loads(v)
        case _:
            raise TypeError(f"Expected str | bytes, got {type(v)}")


def isoparse(timestamp: str) -> DateTime:
    """Parse iso8601 string to datetime."""
    return pendulum.instance(dateutil.parser.isoparse(timestamp))


def in_timezone(timestamp: str | datetime | DateTime, timezone: str) -> DateTime:
    """Parse iso8601 timestamp or DateTime object to DateTime in specified timezone."""
    match timestamp:
        case str():
            return isoparse(timestamp).in_timezone(timezone).replace(tzinfo=None)
        case datetime():
            return pendulum.instance(timestamp).in_timezone(timezone).replace(tzinfo=None)
        case DateTime():
            return timestamp.in_timezone(timezone).replace(tzinfo=None)
        case _:
            raise TypeError(f"Expected str | datetime | DateTime, got {type(timestamp)}")


def split_range(
    start_time: datetime,
    end_time: datetime,
    dt: timedelta
) -> Tuple[List[datetime], List[datetime]]:
    """Split a time range into smaller ranges."""
    start_times = []
    end_times = []
    
    while start_time < end_time:
        start_times.append(start_time)
        next_timestamp = start_time + dt
        
        if next_timestamp >= end_time:
            start_time = end_time
        
        else:
            start_time = next_timestamp
        end_times.append(start_time)
    
    return start_times, end_times


def get_timestamp_index(data: List[Dict[str, Any]]) -> List[str]:
    """Create a single, sorted timestamp index from a chunk of timeseries
    data potentially containing duplicate timestamps.
    
    Duplicate timestamps are removed.
    """
    index = set()
    for datum in data:
        index.update(datum["timestamp"])
    return sorted(index)


def iter_timeseries_rows(
    index: List[str],
    data: List[Dict[str, List[Any]]],
    timezone: str | None = None
) -> Iterable[TimeseriesRow]:
    """Iterate a collection of timeseries data row by row and produce rows
    which have data aligned on a common timestamp.

    Note: The data must be in monotonically increasing order for this to work
    correctly.
    """
    for timestamp in index:
        row = []
        for datum in data:
            try:
                if datum["timestamp"][0] == timestamp:
                    row.append(datum["value"].pop(0))
                    datum["timestamp"].pop(0)
                else:
                    # Most recent data point is later than current timestamp
                    row.append(None)
            except IndexError:
                # No more data for that web id
                row.append(None)
        if timezone is not None:
            timestamp = in_timezone(timestamp, timezone)
        if isinstance(timestamp, str):
            timestamp = isoparse(timestamp)
        yield timestamp, row


class EqualJitterBackoff:
    """Equal jitter backoff upon failure"""

    def __init__(self, max: float, initial: float):
        self.max = max
        self.initial = initial

    def compute(self, failures):
        temp = min(self.max, self.initial * 2**failures) / 2
        return temp + random.uniform(0, temp)