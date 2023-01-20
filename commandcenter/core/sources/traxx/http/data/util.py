import csv
import io
import logging
from collections.abc import Awaitable, Iterable
from typing import Dict, List, Optional, Tuple

from aiohttp import ClientResponse, ClientResponseError

from commandcenter.core.sources.traxx.exceptions import TraxxExpiredSession
from commandcenter.core.integrations.types import JSONPrimitive



_LOGGER = logging.getLogger("commandcenter.core.sources.traxx")


def format_sensor_data(reader: Iterable[Tuple[str, JSONPrimitive]]) -> Dict[str, List[JSONPrimitive]]:
    """Extract timestamp and value for row in the CSV reader."""
    formatted = {"timestamp": [], "value": []}
    if reader is None:
        return formatted
    for line in reader:
        formatted["timestamp"].append(line[0].split("+")[0].strip()) # ex. 2023-01-16 12:00:00 +0000
        formatted["value"].append(line[1])
    return formatted


async def handle_request(
    coro: Awaitable[ClientResponse],
    raise_for_status: bool = True
) -> Optional[Iterable[Tuple[str, JSONPrimitive]]]:
    """Primary response handling for all HTTP requests to Traxx."""
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
        content = await ctx.read()
    
    if not content:
        return None
    elif b"<!DOCTYPE html>" in content:
        raise TraxxExpiredSession()

    buffer = io.StringIO(content.decode())
    return csv.reader(buffer)