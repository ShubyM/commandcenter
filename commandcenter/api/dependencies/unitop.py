from typing import Dict, List

from fastapi import Depends, Request
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pydantic import Json

from commandcenter.api.config.unitops import (
    CC_UNITOPS_COLLECTION_NAME,
    CC_UNITOPS_DATABASE_NAME
)
from commandcenter.api.config.scopes import (
    CC_SCOPES_PIWEB_ACCESS,
    CC_SCOPES_PIWEB_ALLOW_ANY,
    CC_SCOPES_PIWEB_RAISE_ON_NONE,
    CC_SCOPES_TRAXX_ACCESS,
    CC_SCOPES_TRAXX_ALLOW_ANY,
    CC_SCOPES_TRAXX_RAISE_ON_NONE
)
from commandcenter.api.dependencies.db import get_database_connection
from commandcenter.api.dependencies.integrations import get_manager
from commandcenter.auth import requires
from commandcenter.context import set_source
from commandcenter.integrations import AnySubscriptionRequest, Manager, Subscriber
from commandcenter.sources import Sources
from commandcenter.unitops import (
    UnitOp,
    delete_unitop,
    find_unitop,
    find_unitops,
    update_unitop
)



SOURCE_REQUIRES = {
    Sources.PI_WEB_API: requires(
        scopes=list(CC_SCOPES_PIWEB_ACCESS),
        any_=CC_SCOPES_PIWEB_ALLOW_ANY,
        raise_on_no_scopes=CC_SCOPES_PIWEB_RAISE_ON_NONE
    ),
    Sources.TRAXX: requires(
        scopes=list(CC_SCOPES_TRAXX_ACCESS),
        any_=CC_SCOPES_TRAXX_ALLOW_ANY,
        raise_on_no_scopes=CC_SCOPES_TRAXX_RAISE_ON_NONE
    )
}


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
    return await find_unitop(collection, unitop_id)


async def get_unitops(
    q: Json,
    collection: AsyncIOMotorCollection = Depends(get_unitop_collection)
) -> List[UnitOp]:
    return await find_unitops(collection, q)


async def get_unitop_subscribers(
    request: Request,
    unitop: UnitOp = Depends(get_unitop),
) -> List[Subscriber]:
    """Get subscribers from different sources."""
    subscriptions = AnySubscriptionRequest(
        subscriptions=[subscription for subscription in unitop.data_mapping.values()]
    )
    groups = subscriptions.group()
    managers: Dict[Sources, Manager] = {}
    
    for source in groups.keys():
        # User must be authorized for subscriptions to all sources
        await SOURCE_REQUIRES[source](request=request)
        with set_source(source):
            manager = await get_manager()
            managers[source] = manager
    
            subscribers: List[Subscriber] = []
    for source, subscriptions in groups.items():
        manager = managers[source]
        subscriber = await manager.subscribe(subscriptions)
        subscribers.append(subscriber)
    
    return subscribers


async def save_unitop(
    unitop: UnitOp,
    collection: AsyncIOMotorClient = Depends(get_unitop_collection)
) -> None:
    pass