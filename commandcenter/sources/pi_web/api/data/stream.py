import asyncio
import datetime
from collections.abc import AsyncIterable
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Sequence, Tuple, Union

from commandcenter.sources.pi_web.api.client import PIWebAPI
from commandcenter.sources.pi_web.api.util import (
    format_streams_content,
    handle_request,
    split_interpolated_range,
    split_recorded_range
)
from commandcenter.sources.pi_web.models import PIObjType, PISubscription
from commandcenter.types import JSONContent, JSONPrimitive, TimeseriesRow
from commandcenter.util import get_timestamp_index, iter_timeseries_rows, TIMEZONE



async def get_interpolated(
    client: PIWebAPI,
    subscriptions: Sequence[PISubscription],
    start_time: datetime,
    end_time: Optional[datetime] = None,
    interval: Union[timedelta, int] = 60,
    timezone: str = TIMEZONE
) -> AsyncIterable[TimeseriesRow]:
    """Stream timestamp aligned, interpolated data for a sequence of PI subscriptions.
    
    The first row is the column labels which is the sorted hash of the subscriptions.
    Subsequent rows are unlabeled but the indices of the elements align with
    the indices of header row.

    Args:
        web_ids: The web_ids to stream data for
        start_time: The start time of the batch. This will be the timestamp
            in the first row of data
        end_time: The end time of the batch. This will be the timestamp in the
            last row
        interval: The time interval (in seconds) between successive rows.
        request_chunk_size: The maximum number of rows to be returned from a
            single HTTP request. This splits up the time range into successive
            pieces.
        timezone: The timezone to convert the returned data into. The default is
            the local system timezone.

    Yields:
        row: A `TimeseriesRow`.

    Raises:
        ValueError:
            - If one of the subscriptions is not an allowed obj type. Must
                be either 'point' or 'attribute'.
            - If `start_time` >= `end_time`.
        TypeError: If `interval` is an invalid type. 
        ClientError: Error in `aiohttp.ClientSession`.
    """
    allowed = (PIObjType.ATTRIBUTE, PIObjType.POINT)
    if not all([subscription.obj_type in allowed for subscription in subscriptions]):
        raise ValueError(f"All obj types for subscriptions must be one of {', '.join(allowed)}")
    
    end_time = end_time or datetime.now()
    if start_time >= end_time:
        raise ValueError("'start_time' cannot be greater than or equal to 'end_time'")

    subscriptions = sorted(subscriptions)
    web_ids = [subscription.web_id for subscription in subscriptions]
    
    interval = timedelta(seconds=interval) if isinstance(interval, int) else interval
    if not isinstance(interval, timedelta):
        raise TypeError(f"Interval must be timedelta or int. Got {type(interval)}")
    str_interval = f"{interval.total_seconds()} seconds"

    request_chunk_size = int(150000/len(web_ids))
    start_times, end_times = split_interpolated_range(
        start_time,
        end_time,
        interval,
        request_chunk_size
    )
    
    yield [hash(subscription) for subscription in subscriptions]
    
    for start_time, end_time in zip(start_times, end_times):
        dispatch = [
            handle_request(
                client.streams.get_interpolated(
                    web_id,
                    startTime=start_time,
                    endTime=end_time,
                    timeZone=timezone,
                    interval=str_interval,
                    selectedFields="Items.Timestamp;Items.Value;Items.Good"
                ),
                raise_for_status=False
            ) for web_id in web_ids
        ]
        
        contents = await asyncio.gather(*dispatch)
        data = [format_streams_content(content) for content in contents]
        index = get_timestamp_index(data)

        for timestamp, row in iter_timeseries_rows(index, data, timezone):
            yield timestamp, row


async def get_recorded(
    client: PIWebAPI,
    subscriptions: Sequence[PISubscription],
    start_time: datetime,
    end_time: Optional[datetime] = None,
    scan_rate: float = 5.0,
    timezone: str = TIMEZONE
) -> AsyncIterable[TimeseriesRow]:
    """Stream timestamp aligned, recorded data for a sequence of PI subscriptions.
    
    The first row is the column labels which is the sorted hash of the subscriptions.
    Subsequent rows are unlabeled but the indices of the elements align with
    the indices of header row.

    Args:
        client: The HTTP client instance executing the requests.
        subscriptions: The subscriptions to stream data for.
        start_time: The start time of the batch. This will be the timestamp
            in the first row of data.
        end_time: The end time of the batch. This will be the timestamp in the
            last row.
        request_chunk_size: The maximum number of rows to be returned in single
            HTTP request. The time range is sliced according to this and the
            `scan_rate`.
        scan_rate: The scan rate of the PI server. This along with `request_chunk_size`
            helps slice the time range.
        timezone: The timezone to convert the returned data into. The default is
            the local system timezone.

    Yields:
        row: A `TimeseriesRow`.
    
    Raises:
        ValueError:
            - If one of the subscriptions is not an allowed obj type. Must
                be either 'point' or 'attribute'.
            - If `start_time` >= `end_time`
        ClientError: Error in `aiohttp.ClientSession`.
    """
    allowed = (PIObjType.ATTRIBUTE, PIObjType.POINT)
    if not all([subscription.obj_type in allowed for subscription in subscriptions]):
        raise ValueError(f"All obj types for subscriptions must be one of {', '.join(allowed)}")

    end_time = end_time or datetime.now()
    if start_time >= end_time:
        raise ValueError("'start_time' cannot be greater than or equal to 'end_time'")

    subscriptions = sorted(subscriptions)
    web_ids = [subscription.web_id for subscription in subscriptions]

    request_chunk_size = int(150000/len(web_ids))
    start_times, end_times = split_recorded_range(
        start_time,
        end_time,
        request_chunk_size,
        scan_rate
    )
    
    yield [hash(subscription) for subscription in subscriptions]

    for i, (start_time, end_time) in enumerate(zip(start_times, end_times)):
        first_row, last_row = await _get_recorded_at_times(
            client,
            web_ids,
            start_time,
            end_time
        )

        yield start_time, first_row
        
        dispatch = [
            handle_request(
                client.streams.get_recorded(
                    web_id,
                    startTime=start_time,
                    endTime=end_time,
                    timeZone=timezone,
                    selectedFields="Items.Timestamp;Items.Value;Items.Good"
                ),
                raise_for_status=False
            ) for web_id in web_ids
        ]
        
        contents = await asyncio.gather(*dispatch)
        data = [format_streams_content(content) for content in contents]
        index = get_timestamp_index(data)

        l = len(start_times)-1
        m = len(index)-1
        for j, (timestamp, row) in enumerate(iter_timeseries_rows(index, data)):
            if i == 0 and j == 0:
                if timestamp == start_time:
                    continue
                yield timestamp, row
            elif i == l and j == m:
                if timestamp == end_time:
                    continue
                yield timestamp, row
            else:
                yield timestamp, row
        else:
            yield end_time, last_row


async def _get_recorded_at_times(
    client: PIWebAPI,
    web_ids: List[str],
    start_time: datetime,
    end_time: Optional[datetime],
    timezone: str
) -> Tuple[List[JSONPrimitive], List[JSONPrimitive]]:
    """Returns the first and last rows of a recorded batch."""
    def parse_recorded_at_time_row(data: List[Dict[str, JSONContent]]) -> TimeseriesRow:
        row = []
        for item in data:
            if not item:
                row.append(None)
                continue
            if item["Good"]:
                value = item["Value"]
                if isinstance(value, dict):
                    row.append(value["Name"])
                else:
                    row.append(value)
            else:
                row.append(None)
        return row

    first, last = (
        [
            handle_request(
                client.streams.get_recorded_at_time(
                    web_id,
                    time=start_time,
                    timeZone=timezone,
                    selectedFields="Items.Timestamp;Items.Value;Items.Good"
                ),
                raise_for_status=False
            ) for web_id in web_ids
        ],
        [
            handle_request(
                client.streams.get_recorded_at_time(
                    web_id,
                    time=end_time,
                    timeZone=timezone,
                    selectedFields="Items.Timestamp;Items.Value;Items.Good"
                ),
                raise_for_status=False
            ) for web_id in web_ids
        ]
    )
    data = await asyncio.gather(*first)
    first_row = parse_recorded_at_time_row(start_time, data)
    data = await asyncio.gather(*last)
    last_row = parse_recorded_at_time_row(end_time, data)
    return first_row, last_row