import asyncio
from collections.abc import AsyncIterable, Sequence
from datetime import datetime, timedelta
from typing import Optional, Union

import pendulum

from commandcenter.core.sources.traxx.http.client import TraxxClient
from commandcenter.core.sources.traxx.http.data.util import (
    format_sensor_data,
    handle_request
)
from commandcenter.core.sources.traxx.models import TraxxSubscription
from commandcenter.core.integrations.types import TimeseriesRow
from commandcenter.core.integrations.util import TIMEZONE
from commandcenter.core.integrations.util.common import (
    get_timestamp_index,
    iter_rows,
    split_range
)



async def get_sensor_data(
    client: TraxxClient,
    subscriptions: Sequence[TraxxSubscription],
    start_time: datetime,
    end_time: Optional[datetime] = None,
    interval: Union[timedelta, int] = timedelta(minutes=120),
    timezone: str = TIMEZONE
) -> AsyncIterable[TimeseriesRow]:
    """Stream timestamp aligned, recorded data for a sequence of Traxx subscriptions.
    
    This iterable streams batch data for a sequence of subscriptions row by row. The
    first row is an identifier row specifying the columns for each subsequent
    row returned. Subsequent rows are unlabeled but the indices of the elements
    align with the indices of header row.

    Args:
        client: The HTTP client instance executing the requests.
        subscriptions: The subscriptions to stream data for.
        start_time: The start time of the batch. This will be the timestamp
            in the first row of data.
        end_time: The end time of the batch. This will be the timestamp in the
            last row.
        interval: Chunks large time ranges into smaller ranges so that not all
            the data is requested at once.
        timezone: The timezone to convert the returned data into. The default is
            the local system timezone.

    Yields:
        List[JSONPrimitive]: The data for the row. The first row is always the
            index row
    
    Raises:
        TypeError: If `interval` is an invalid type.
        ClientError: Error in `aiohttp.ClientSession`.
    """
    end_time = end_time or datetime.now()
    if start_time >= end_time:
        raise ValueError("'start_time' cannot be greater than or equal to 'end_time'")

    subscriptions = sorted(subscriptions)
    # The boundary on the start and end are inside boundaries so we add 15 minutes
    # to each side of the start and end time to try and make sure we include
    # those values (Traxx devices transmit data on 14 minute cycles normally)
    start_time_extend =  start_time - timedelta(minutes=15)
    end_time_extend = end_time + timedelta(minutes=15)
    interval = timedelta(minutes=interval) if isinstance(interval, int) else interval

    if not isinstance(interval, timedelta):
        raise TypeError(f"Interval must be timedelta or int. Got {type(interval)}")
    start_times, end_times = split_range(start_time_extend, end_time_extend, interval)

    yield [hash(subscription) for subscription in subscriptions]

    for start_time_, end_time_ in zip(start_times, end_times):
        dispatch = [
            handle_request(
                client.sensors.sensor_data(
                    subscription.asset_id,
                    subscription.sensor_id,
                    begin=int(pendulum.instance(start_time_, timezone).float_timestamp * 1000),
                    end=int(pendulum.instance(end_time_, timezone).float_timestamp * 1000),
                    tz=timezone
                ),
                raise_for_status=False
            ) for subscription in subscriptions
        ]

        readers = await asyncio.gather(*dispatch)
        data = [format_sensor_data(reader) for reader in readers]
        index = get_timestamp_index(data)

        last_timestamp = None
        last_row = None
        for timestamp, row in iter_rows(index, data, timezone):
            if timestamp < start_time:
                continue
            elif timestamp > start_time and last_timestamp is not None and last_timestamp < start_time:
                yield start_time, last_row
            elif timestamp > end_time and last_timestamp is not None and last_timestamp < end_time:
                yield end_time, row
                return
            else:
                yield timestamp, row

            last_timestamp = timestamp
            last_row = row