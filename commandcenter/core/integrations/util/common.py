import json
from collections.abc import Iterable
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union

import dateutil.parser
import orjson
import pendulum
from pendulum.datetime import DateTime

from commandcenter.core.integrations.types import JSONPrimitive, TimeseriesRow


def to_camel(string: str) -> str:
    """Covert snake case (arg_a) to camel case (ArgA)."""
    return ''.join(word.capitalize() for word in string.split('_'))


def isoparse(timestamp: str) -> DateTime:
    """Parse iso8601 string to datetime."""
    return pendulum.instance(dateutil.parser.isoparse(timestamp))


def in_timezone(timestamp: Union[str, DateTime], timezone: str) -> DateTime:
    """Parse iso8601 timestamp or DateTime object to DateTime in specified timezone."""
    if isinstance(timestamp, str):
        return isoparse(timestamp).in_timezone(timezone).replace(tzinfo=None)
    return timestamp.in_timezone(timezone).replace(tzinfo=None)


def json_loads(v: Union[str, bytes]):
    """JSON decoder which uses orjson for bytes and builtin json for str."""
    if isinstance(v, str):
        return json.loads(v)
    return orjson.loads(v)


def split_range(
    start_time: datetime,
    end_time: datetime,
    dt: timedelta
) -> Tuple[List[datetime], List[datetime]]:
    """Split a time range into smaller ranges"""
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


def get_timestamp_index(data: List[Dict[str, JSONPrimitive]]) -> List[str]:
    """Create a single, sorted timestamp index from all timestamps returned
    from streams data.
    
    Duplicate timestamps are removed.
    """
    index = set()
    for datum in data:
        index.update(datum["timestamp"])
    return sorted(index)


def iter_rows(
    index: List[str],
    data: List[Dict[str, List[JSONPrimitive]]],
    timezone: Optional[str] = None
) -> Iterable[TimeseriesRow]:
    """Iterate through the data for each web_id row by row and produce rows
    which have data aligned on a common timestamp.
    """
    for timestamp in index:
        if timezone is not None:
            timestamp = in_timezone(timestamp, timezone)
        if isinstance(timestamp, str):
            timestamp = isoparse(timestamp)
        
        iso_timestamp = timestamp.isoformat()
        
        row = []
        for datum in data:
            try:
                if datum["timestamp"][0] == iso_timestamp:
                    row.append(datum["value"].pop(0))
                    datum["timestamp"].pop(0)
                else:
                    # Most recent data point is later than current timestamp
                    row.append(None)
            except IndexError:
                # No more data for that web id
                row.append(None)
        yield timestamp, row