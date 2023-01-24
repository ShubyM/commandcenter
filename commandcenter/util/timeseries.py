from collections.abc import Iterable
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

import dateutil.parser
import pendulum
from pendulum.datetime import DateTime

from commandcenter.types import TimeseriesRow


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