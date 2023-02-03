import asyncio
import math
from collections.abc import AsyncIterable
from datetime import datetime, timedelta
from typing import Dict, List, Set

from motor.motor_asyncio import AsyncIOMotorClient

from commandcenter.integrations.models import Subscription
from commandcenter.types import JSONPrimitive, TimeseriesRow
from commandcenter.util import (
    get_timestamp_index,
    iter_timeseries_rows,
    split_range
)


def format_timeseries_content(
    content: List[Dict[str, datetime | JSONPrimitive]]
) -> Dict[str, List[JSONPrimitive]]:
    formatted = {"timestamp": [], "value": []}
    for item in content:
        formatted["timestamp"].append(item["timestamp"])
        formatted["value"].append(item["value"])
    return formatted


async def get_timeseries(
    client: AsyncIOMotorClient,
    subscriptions: Set[Subscription],
    database_name: str,
    start_time: datetime,
    end_time: datetime | None = None,
    scan_rate: int = 5
) -> AsyncIterable[TimeseriesRow]:
    end_time = end_time or datetime.now()
    if start_time >= end_time:
        raise ValueError("'start_time' cannot be greater than or equal to 'end_time'")
    
    request_chunk_size = min(int(150_000/len(subscriptions)), 10_000)

    td = end_time - start_time
    request_time_range = td.total_seconds()
    items_requested = math.ceil(request_time_range/scan_rate)
    
    if items_requested <= request_chunk_size:
        start_times, end_times = [start_time], [end_time]
    else:
        dt = timedelta(seconds=math.floor(request_chunk_size*scan_rate))
        start_times, end_times = split_range(start_time, end_time, dt)
    
    collection = client[database_name]["timeseries_data"]

    hashes = sorted(subscriptions)
    #hashes = [hash(subscription) for subscription in subscriptions]

    for start_time, end_time in zip(start_times, end_times):
        dispatch = [
            collection.find(
                {
                    "timestamp": {"$gte": start_time, "$lt": end_time},
                    "subscription": hash_
                },
                {"timestamp": 1, "value": 1, "_id": 0}
            ).sort("timestamp", 1).to_list(None) for hash_ in hashes
        ]
        contents = await asyncio.gather(*dispatch)
        data = [format_timeseries_content(content) for content in contents]
        index = get_timestamp_index(data)

        for timestamp, row in iter_timeseries_rows(index, data):
            yield timestamp, row