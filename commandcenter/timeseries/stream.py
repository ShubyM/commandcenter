import asyncio
from collections.abc import AsyncIterable
from datetime import datetime
from typing import Dict, List, Set

from motor.motor_asyncio import AsyncIOMotorClient

from commandcenter.integrations.models import Subscription
from commandcenter.types import JSONPrimitive, TimeseriesRow
from commandcenter.util import (
    get_timestamp_index,
    iter_timeseries_rows,
    split_recorded_range
)



def format_timeseries_content(
    content: List[Dict[str, datetime | JSONPrimitive]]
) -> Dict[str, List[JSONPrimitive]]:
    """Format query results for iteration."""
    formatted = {"timestamp": [], "value": []}
    for item in content:
        formatted["timestamp"].append(item["timestamp"])
        formatted["value"].append(item["value"])
    return formatted


async def get_timeseries(
    client: AsyncIOMotorClient,
    subscriptions: Set[Subscription],
    database_name: str,
    collection_name: str,
    start_time: datetime,
    end_time: datetime | None = None,
    scan_rate: int = 5
) -> AsyncIterable[TimeseriesRow]:
    """Stream timestamp aligned data for a sequence of subscriptions.
    
    The subscriptions are sorted according to their hash. Row indices align
    with the hash order.

    Args:
        client: The motor client.
        subscriptions: The subscriptions to stream data for.
        database_name: The database to query.
        collection_name: The collection to query.
        start_time: Start time of query. This is inclusive.
        end_time: End time of query.
        interval: The time interval (in seconds) between successive rows.
        scan_rate: A representative number of the data update frequency.

    Yields:
        row: A `TimeseriesRow`.

    Raises:
        ValueError: If 'start_time' >= 'end_time'.
        PyMongoError: Error in motor client.
    """
    end_time = end_time or datetime.now()
    if start_time >= end_time:
        raise ValueError("'start_time' cannot be greater than or equal to 'end_time'")

    request_chunk_size = min(int(150_000/len(subscriptions)), 10_000)
    hashes = [hash(subscription) for subscription in sorted(subscriptions)]
    start_times, end_times = split_recorded_range(
        start_time=start_time,
        end_time=end_time,
        request_chunk_size=request_chunk_size,
        scan_rate=scan_rate
    )
    
    collection = client[database_name][collection_name]

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