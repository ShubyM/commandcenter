import logging
from collections.abc import Awaitable
from typing import Dict, List, Optional

import orjson
from aiohttp import ClientResponse, ClientResponseError

from commandcenter.sources.pi_web.exceptions import PIWebResponseError
from commandcenter.types import JSONContent, JSONPrimitive



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