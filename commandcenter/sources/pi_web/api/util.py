import logging
import math
from collections.abc import Awaitable
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import orjson
from aiohttp import ClientResponse, ClientResponseError

from commandcenter.exceptions import PIWebResponseError
from commandcenter.types import JSONContent, JSONPrimitive
from commandcenter.util import split_range


_LOGGER = logging.getLogger("commandcenter.sources.pi_web")


def format_streams_content(content: Dict[str, List[Dict[str, JSONPrimitive]]]) -> Dict[str, List[JSONPrimitive]]:
    """Extract timestamp and value for each item in a stream."""
    formatted = {"timestamp": [], "value": []}
    items = content.get("Items", []) if content is not None else []
    
    for item in items:
        timestamp = item["Timestamp"]
        good = item["Good"]
        if not good:
            value = None
        else:
            # if a stream item returned an error, the value will be None anyway
            # and we're not particularly interested in the errors
            # https://docs.osisoft.com/bundle/pi-web-api-reference/page/help/topics/error-handling.html
            value = item["Value"]
            if isinstance(value, dict):
                value = value["Name"]
        formatted["timestamp"].append(timestamp)
        formatted["value"].append(value)
    
    return formatted


def split_interpolated_range(
    start_time: datetime,
    end_time: datetime,
    interval: timedelta,
    request_chunk_size: int = 5000
) -> Tuple[List[datetime], List[datetime]]:
    """Split a time range into smaller ranges for interpolated requests."""
    td: timedelta = end_time - start_time
    request_time_range = td.total_seconds()
    items_requested = math.ceil(
        request_time_range/interval.total_seconds()
    )
    
    if items_requested <= request_chunk_size:
        return [start_time], [end_time]
    
    # Derive an interval that will produce request_chunk_size items per request
    dt = timedelta(seconds=math.floor(interval.total_seconds()*request_chunk_size))
    return split_range(start_time, end_time, dt)


def split_recorded_range(
    start_time: datetime,
    end_time: datetime,
    request_chunk_size: int = 5000,
    scan_rate: float = 5
) -> Tuple[List[datetime], List[datetime]]:
    """Split a time range into smaller ranges for recorded requests."""
    td: timedelta = end_time - start_time
    request_time_range = td.total_seconds()
    items_requested = math.ceil(request_time_range/scan_rate)
    
    if items_requested <= request_chunk_size:
        return [start_time], [end_time]
    
    # Derive an interval that will produce (at most) request_chunk_size items per
    # request. The total items returned for a recorded request is not determinisitic
    dt = timedelta(seconds=math.floor(request_chunk_size*scan_rate))
    return split_range(start_time, end_time, dt)


async def handle_request(
    coro: Awaitable[ClientResponse],
    raise_for_status: bool = True,
    raise_for_error: bool = True
) -> Optional[JSONContent]:
    """Primary response handling for all HTTP requests to the PI Web API."""
    response = await coro
    
    try:
        response.raise_for_status()
    except ClientResponseError as err:
        await response.release()
        if raise_for_status:
            raise
        _LOGGER.warning("Error in client response (%i)", err.code, exc_info=True)
        return None
    
    async with response as ctx:
        data: JSONContent = await ctx.json(loads=orjson.loads)
    errors = data.get("Errors")
    
    if errors:
        if raise_for_error:
            raise PIWebResponseError(errors)
        else:
            _LOGGER.warning(
                "%i errors returned in response body",
                len(errors),
                extra={
                    "errors": ", ".join(errors),
                    "status": response.status,
                    "endpoint": response.url
                }
            )
            return None
    
    return data