import asyncio
import logging
from typing import List, Sequence, Optional, Tuple

from pydantic import ValidationError

from commandcenter.core.sources.pi_web.exceptions import ContentError
from commandcenter.core.sources.pi_web.http.client import PIWebClient
from commandcenter.core.sources.pi_web.http.data.util import handle_request
from commandcenter.core.sources.pi_web.models import (
    PIObjSearch,
    PISubscription
)
from commandcenter.core.integrations.types import JSONContent
from commandcenter.core.objcache import memo



_LOGGER = logging.getLogger("commandcenter.core.sources.pi_web")


@memo
async def get_dataserver_webid(_client: PIWebClient, dataserver: Optional[str] = None) -> str:
    """Get the dataserver WebId.

    If no dataserver is given, the first one in the list will be returned.

    Raises:
        ClientException: Error in `aiohttp.ClientSession`.
        ContentError: Query returned no results.
    """
    data = await handle_request(
        _client.dataservers.list(selectedFields="Items.Name;Items.WebId")    
    )
    items = data.get("Items")
    if not items or not isinstance(items, list):
        raise ContentError(
            "Could not get dataserver WebId. No items returned in response"
        )
    if dataserver:
        for item in items:
            if item["WebId"] == dataserver:
                return item["WebId"]
        else:
            raise ContentError(f"WebId not found for '{dataserver}'")
    else:
        return items[0]["WebId"]


async def search_points(
    client: PIWebClient,
    points: Sequence[PIObjSearch]
) -> Tuple[List[PISubscription], List[PIObjSearch]]:
    """Get the WebId for a sequence of pi points.
    
    If a point is not found or the query returns multiple results, the query for
    for that point will fail. Therefore you cannot use wild card searches
    for this method (unless the wild card search returns 1 point).

    Note: This will eventually be deprecated for a more universal search tool
    through the AF SDK. We have to search for the WebId's through the dataserver
    instead of the /search endpoint because the crawler for the indexed search
    is unreliable in some versions of the PI Web API and frequently goes down.
    """
    points = list(points)
    
    dispatch = [get_dataserver_webid(client, point.server) for point in points]
    dataserver_web_ids = await asyncio.gather(*dispatch)
    
    dispatch = [
        handle_request(
            client.dataservers.get_points(
                dataserver_web_id,
                nameFilter=point.search,
                selectedFields="Items.Name;Items.WebId",
                webIdType=point.web_id_type
            )
        ) for dataserver_web_id, point in zip(dataserver_web_ids, points)
    ]
    results: List[JSONContent] = await asyncio.gather(*dispatch)
    subs: List[PISubscription] = []
    failed: List[PIObjSearch] = []
    
    for point, result in zip(points, results):
        items = result.get("Items")
        if not items or not isinstance(items, list) or len(items) > 1:
            failed.append(point)
        else:
            try:
                sub = point.to_subscription(web_id=items[0]["WebId"], name=items[0]["Name"])
                subs.append(sub)
            except ValidationError:
                _LOGGER.warning("Subscription validation failed", exc_info=True, extra={"raw": items})
                failed.append(point)
    
    return subs, failed