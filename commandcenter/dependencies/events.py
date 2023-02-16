from fastapi import Depends, HTTPException, status
from jsonschema import ValidationError, validate
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection

from commandcenter.config.events import (
    CC_EVENTS_COLLECTION_NAME,
    CC_EVENTS_DATABASE_NAME,
    CC_TOPICS_COLLECTION_NAME,
    CC_TOPICS_DATABASE_NAME
)
from commandcenter.dependencies.db import get_database_connection
from commandcenter.setup.events import setup_event_bus, setup_event_handler
from commandcenter.events import (
    Event,
    EventBus,
    EventQueryResult,
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
    topic: str,
    collection: AsyncIOMotorCollection = Depends(get_topics_collection)
) -> Topic:
    """Retrieve a topic by its name."""
    document = await collection.find_one({"topic": topic})
    if document is not None:
        return Topic.parse_obj(document)
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Topic '{topic}' not found."
    )


async def list_topics(
    collection: AsyncIOMotorCollection = Depends(get_topics_collection)
) -> TopicQueryResult:
    """Retrieve a list of all topics."""
    documents = await collection.find(projection={"topic": 1, "_id": 0}).to_list(None)
    if documents:
        return TopicQueryResult(items=[document["topic"] for document in documents])
    return TopicQueryResult(items=[])


async def validate_event(
    event: Event,
    topic_: Topic = Depends(get_topic)
) -> Event:
    """Validate an event payload against the topic schema."""
    try:
        validate(event.payload, topic_.schema_)
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"{e.json_path}-{e.message}"
        )
    return event


async def get_events(
    collection: AsyncIOMotorCollection,
    topic: str,
    routing_key: str | None = None,
    n: int = 500,
) -> EventQueryResult:
    q = {k: v for k, v in {"topic": topic, "routing_key": routing_key}.items() if v}
    documents = await collection.find(q).sort("timestamp", -1).limit(n).to_list(None)
    if documents:
        return EventQueryResult(items=documents)
    return EventQueryResult(items=[])


async def get_n_events(
    topic: str,
    routing_key: str | None = None,
    n: int = 500,
    collection: AsyncIOMotorCollection = Depends(get_events_collection)
) -> EventQueryResult:
    """Get last N events for a topic-routing key combination."""
    return await get_events(
        collection=collection,
        topic=topic,
        routing_key=routing_key,
        n=n
    )


async def get_last_event(
    topic: str,
    routing_key: str | None = None,
    collection: AsyncIOMotorCollection = Depends(get_events_collection)
) -> Event:
    """Get last event for a topic-routing key combination."""
    result = await get_events(
        collection=collection,
        topic=topic,
        routing_key=routing_key,
        n=1
    )
    if not result.items:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No events found matching criteria."
        )
    return result.items[0]