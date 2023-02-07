from fastapi import Depends, HTTPException, status
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection

from commandcenter.api.config.events import (
    CC_EVENTS_COLLECTION_NAME,
    CC_EVENTS_DATABASE_NAME,
    CC_TOPICS_COLLECTION_NAME,
    CC_TOPICS_DATABASE_NAME
)
from commandcenter.api.dependencies.db import get_database_connection
from commandcenter.api.setup.events import setup_event_bus, setup_event_handler
from commandcenter.events import (
    EventBus,
    MongoEventHandler,
    Topic,
    TopicQueryResult
)



def get_event_handler() -> MongoEventHandler:
    """Dependency for retrieving an event handler."""
    return setup_event_handler()


async def get_event_bus() -> EventBus:
    """Dependency for retrieving an event bus."""
    return setup_event_bus()


async def get_events_collection(
    client: AsyncIOMotorClient = Depends(get_database_connection)
) -> AsyncIOMotorCollection:
    """Open a database connection and get the events collection."""
    return client[CC_EVENTS_DATABASE_NAME][CC_EVENTS_COLLECTION_NAME]


async def get_topics_collection(
    client: AsyncIOMotorClient = Depends(get_database_connection)
) -> AsyncIOMotorCollection:
    """Open a database connection and get the topics collection."""
    return client[CC_TOPICS_DATABASE_NAME][CC_TOPICS_COLLECTION_NAME]


async def get_topic(
    name: str,
    collection: AsyncIOMotorCollection = Depends(get_topics_collection)
) -> Topic:
    """Retrieve a topic by its name."""
    document = await collection.find_one({"name": name})
    if document is not None:
        return Topic.parse_obj(document)
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Topic '{name}' not found."
    )


async def list_topics(
    collection: AsyncIOMotorCollection = Depends(get_topics_collection)
) -> TopicQueryResult:
    """Retrieve a list of all topics."""
    documents = await collection.find(projection={"name": 1, "_id": 0}).to_list()
    if documents:
        TopicQueryResult(items=[document["name"] for document in documents])
    return TopicQueryResult(items=[])