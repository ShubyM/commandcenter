import asyncio
import logging
from typing import List, Sequence, Optional

from pydantic import ValidationError

from commandcenter.caching import memo
from commandcenter.exceptions import PIWebContentError
from commandcenter.sources.pi_web.api.client import PIWebAPI
from commandcenter.sources.pi_web.api.util import handle_request
from commandcenter.sources.pi_web.models import (
    PIObjSearch,
    PIObjSearchFailed,
    PIObjSearchResult,
    PISubscription
)
from commandcenter.types import JSONContent



_LOGGER = logging.getLogger("commandcenter.sources.pi_web.search")


@memo
async def get_dataserver_webid(_client: PIWebAPI, dataserver: Optional[str] = None) -> str:
    """Get the dataserver WebId.

    If no dataserver is given, the first one in the list will be returned.

    Raises:
        ClientException: Error in `aiohttp.ClientSession`.
        PIWebContentError: Query returned no results.
    """
    data = await handle_request(
        _client.dataservers.list(selectedFields="Items.Name;Items.WebId")    
    )
    items = data.get("Items")
    if not items or not isinstance(items, list):
        raise PIWebContentError(
            "Could not get dataserver WebId. No items returned in response"
        )
    if dataserver:
        for item in items:
            if item["WebId"] == dataserver:
                return item["WebId"]
        else:
            raise PIWebContentError(f"WebId not found for '{dataserver}'")
    else:
        return items[0]["WebId"]


async def search_points(
    client: PIWebAPI,
    points: Sequence[PIObjSearch]
) -> PIObjSearchResult:
    """Get the WebId for a sequence of pi points.
    
    If a point is not found or the query returns multiple results, the query for
    for that point will fail. Therefore you cannot use wild card searches
    for this method (unless the wild card search returns 1 point).

    Note: This will eventually be deprecated for a more universal search tool
    through the AF SDK. We have to search for the WebId's through the dataserver
    instead of the '/search' endpoint because the crawler for the indexed search
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
                webIdType=point.web_id_type.value
            ),
            raise_for_status=False,
            raise_for_error=False
        ) for dataserver_web_id, point in zip(dataserver_web_ids, points)
    ]
    results: List[JSONContent] = await asyncio.gather(*dispatch)
    subscriptions: List[PISubscription] = []
    failed: List[PIObjSearch] = []
    
    for point, result in zip(points, results):
        if not result:
            failed.append(
                PIObjSearchFailed(
                    obj=point,
                    reason="Search returned no results."
                )
            )
            continue
        items = result.get("Items")
        if not isinstance(items, list):
            _LOGGER.warning(
                "Search returned unhandled data type %s",
                type(items),
                extra={"search": point.dict()}
            )
            failed.append(
                PIObjSearchFailed(
                    obj=point,
                    reason="Search returned unhandled data type."
                )
            )
        elif not items:
            failed.append(
                PIObjSearchFailed(
                    obj=point,
                    reason="Search returned no results."
                )
            )
        elif len(items) > 1:
            failed.append(
                PIObjSearchFailed(
                    obj=point,
                    reason="Search returned more than one result.")
                )
        else:
            try:
                sub = point.to_subscription(
                    web_id=items[0]["WebId"],
                    name=items[0]["Name"]
                )
                subscriptions.append(sub)
            except ValidationError:
                _LOGGER.warning(
                    "Subscription validation failed",
                    exc_info=True,
                    extra={
                        "raw": items,
                        "search": point.dict()
                    }
                )
                failed.append(
                    PIObjSearchFailed(
                        obj=point,
                        reason="Failed to convert search result into a subscription."
                    )
                )

    return PIObjSearchResult(subscriptions=subscriptions, failed=failed)