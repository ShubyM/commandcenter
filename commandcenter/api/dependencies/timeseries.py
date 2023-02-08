from typing import List

from fastapi import Depends, HTTPException, Request, status
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pydantic import Json

from commandcenter.api.config.timeseries import (
    CC_TIMESERIES_DATABASE_NAME,
    CC_TIMESERIES_COLLECTION_NAME,
    CC_UNITOPS_COLLECTION_NAME,
    CC_UNITOPS_DATABASE_NAME
)
from commandcenter.api.dependencies.db import get_database_connection
from commandcenter.api.dependencies.integrations import get_manager
from commandcenter.api.dependencies.sources import is_authorized
from commandcenter.api.setup.timeseries import setup_timeseries_handler
from commandcenter.context import set_source
from commandcenter.integrations import AnySubscriptionRequest, Subscriber
from commandcenter.timeseries import (
    MongoTimeseriesHandler,
    UnitOp,
    UnitOpQueryResult
)



def get_timeseries_handler() -> MongoTimeseriesHandler:
    """Dependency for retrieving a timeseries handler."""
    return setup_timeseries_handler()


async def get_timeseries_collection(
    client: AsyncIOMotorClient = Depends(get_database_connection)
) -> AsyncIOMotorCollection:
    """Open a database connection and get the timeseries data collection."""
    return client[CC_TIMESERIES_DATABASE_NAME][CC_TIMESERIES_COLLECTION_NAME]


async def get_unitop_collection(
    client: AsyncIOMotorClient = Depends(get_database_connection)
) -> AsyncIOMotorCollection:
    """Open a database connection and get the unitop collection."""
    return client[CC_UNITOPS_DATABASE_NAME][CC_UNITOPS_COLLECTION_NAME]


async def get_unitop(
    unitop_id: str,
    collection: AsyncIOMotorCollection = Depends(get_unitop_collection)
) -> UnitOp:
    """Retrieve a unitop by its ID."""
    document = await collection.find_one({"unitop_id": unitop_id})
    if document is not None:
        return UnitOp.parse_obj(document)
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Unitop ID '{unitop_id}' not found."
    )


async def get_unitops(
    q: Json,
    collection: AsyncIOMotorCollection = Depends(get_unitop_collection)
) -> UnitOpQueryResult:
    """Retrieve a list of unitops from a query."""
    try:
        documents = await collection.find(q).to_list(None)
    except TypeError: # Invalid query
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid query.")
    if documents:
        return UnitOpQueryResult(items=documents)
    return UnitOpQueryResult(items=[])


async def get_unitop_and_authorize(
    request: Request,
    unitop: UnitOp = Depends(get_unitop)
) -> UnitOp:
    """Retrieves a unit op and verifies user is authorized to access data from
    all subscription sources.
    """
    subscriptions = AnySubscriptionRequest(subscriptions=list(unitop.data_mapping.values()))
    groups = subscriptions.group()
    for source in groups.keys():
        # User must be authorized for subscriptions to all sources
        await is_authorized(request=request, source=source)
    else:
        return unitop


async def get_unitop_subscribers(
    request: Request,
    unitop: UnitOp = Depends(get_unitop),
) -> List[Subscriber]:
    """Get subscribers from different sources."""
    subscriptions = AnySubscriptionRequest(
        subscriptions=[subscription for subscription in unitop.data_mapping.values()]
    )
    groups = subscriptions.group()
    managers = {}
    
    for source in groups.keys():
        # User must be authorized for subscriptions to all sources
        await is_authorized(request=request, source=source)
        with set_source(source):
            manager = await get_manager()
            managers[source] = manager
    
            subscribers: List[Subscriber] = []
    for source, subscriptions in groups.items():
        manager = managers[source]
        subscriber = await manager.subscribe(subscriptions)
        subscribers.append(subscriber)
    
    return subscribers