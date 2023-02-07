import logging
from datetime import datetime
from typing import List

import anyio
from fastapi import APIRouter, Depends, HTTPException, Query, WebSocket, status
from fastapi.responses import JSONResponse, StreamingResponse
from motor.motor_asyncio import AsyncIOMotorCollection
from sse_starlette import EventSourceResponse

from commandcenter.api.config.scopes import (
    COMMANDCENTER_READ_ACCESS,
    COMMANDCENTER_READ_ALLOW_ANY,
    COMMANDCENTER_READ_RAISE_ON_NONE,
    COMMANDCENTER_WRITE_ACCESS,
    COMMANDCENTER_WRITE_ALLOW_ANY,
    COMMANDCENTER_WRITE_RAISE_ON_NONE
)
from commandcenter.api.dependencies import (
    get_event_bus,
    get_event_handler,
    get_events_collection,
    get_topic,
    get_topics_collection,
    list_topics,
    requires
)
from commandcenter.api.models import Status, StatusOptions
from commandcenter.auth import BaseUser
from commandcenter.events import (
    Event,
    EventBus,
    MongoEventHandler,
    Topic,
    TopicQueryResult,
    TopicSubscription
)
from commandcenter.util import (
    FileWriter,
    chunked_transfer,
    format_timeseries_rows,
    sse_handler,
    ws_handler
)



_LOGGER = logging.getLogger("commandcenter.api.events")

router = APIRouter(
    prefix="/events",
    dependencies=[
        Depends(
            requires(
                scopes=COMMANDCENTER_READ_ACCESS,
                any_=COMMANDCENTER_READ_ALLOW_ANY,
                raise_on_no_scopes=COMMANDCENTER_READ_RAISE_ON_NONE
            )
        )
    ],
    tags=["Events"]
)


@router.get("/topics/{name}", response_model=Topic)
async def topic(
    topic: Topic = Depends(get_topic)
) -> Topic:
    """Retrieve a topic record."""
    return topic


@router.get("/topics", response_model=TopicQueryResult)
async def topics(
    topics: TopicQueryResult = Depends(list_topics)
) -> TopicQueryResult:
    """Retrieve a collection of unitop records."""
    return topics


@router.post(
    "/topics/save",
    response_model=Status,
    dependencies=[
        Depends(
            requires(
                scopes=COMMANDCENTER_WRITE_ACCESS,
                any_=COMMANDCENTER_WRITE_ALLOW_ANY,
                raise_on_no_scopes=COMMANDCENTER_WRITE_RAISE_ON_NONE
            )
        )
    ]
)
async def save(
    topic: Topic,
    collection: AsyncIOMotorCollection = Depends(get_topics_collection)
) -> Status:
    """Save a topic to the database."""
    result = await collection.update_one(
        {"name": topic.name},
        topic.dict(),
        upsert=True
    )
    if result.modified_count > 0 or result.matched_count > 0:
        return Status(status=StatusOptions.OK)
    return Status(status=StatusOptions.FAILED)


