import logging

from fastapi import APIRouter, BackgroundTasks, Depends, WebSocket
from motor.motor_asyncio import AsyncIOMotorCollection
from sse_starlette import EventSourceResponse

from commandcenter.config.scopes import (
    COMMANDCENTER_READ_ACCESS,
    COMMANDCENTER_READ_ALLOW_ANY,
    COMMANDCENTER_READ_RAISE_ON_NONE,
    COMMANDCENTER_WRITE_ACCESS,
    COMMANDCENTER_WRITE_ALLOW_ANY,
    COMMANDCENTER_WRITE_RAISE_ON_NONE
)
from commandcenter.dependencies import (
    get_cached_reference,
    get_event_bus,
    get_event_handler,
    get_last_event,
    get_n_events,
    get_reference_token,
    get_topic,
    get_topics_collection,
    list_topics,
    requires,
    validate_event
)
from commandcenter.auth import BaseUser
from commandcenter.caching.tokens import ReferenceToken
from commandcenter.events import (
    Event,
    EventBus,
    EventQueryResult,
    MongoEventHandler,
    EventSubscriptionError,
    Topic,
    TopicQueryResult,
    TopicSubscriptionRequest
)
from commandcenter.integrations import iter_subscriber
from commandcenter.util import Status, StatusOptions, sse_handler, ws_handler



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


@router.get("/topics/{topic}", response_model=Topic)
async def topic(
    topic_: Topic = Depends(get_topic)
) -> Topic:
    """Retrieve a topic record."""
    return topic_


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
    result = await collection.replace_one(
        {"topic": topic.topic},
        topic.dict(by_alias=True),
        upsert=True
    )
    if result.modified_count > 0 or result.matched_count > 0:
        return Status(status=StatusOptions.OK)
    return Status(status=StatusOptions.FAILED)


@router.post(
    "/publish/{topic}",
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
async def publish(
    tasks: BackgroundTasks,
    event: Event = Depends(validate_event),
    bus: EventBus = Depends(get_event_bus),
    handler: MongoEventHandler = Depends(get_event_handler)
) -> Status:
    """Publish an event to the bus."""
    if bus.publish(event):
        tasks.add_task(handler.publish, event.to_document())
        return Status(status=StatusOptions.OK)
    return Status(status=StatusOptions.FAILED)


@router.post("/subscribe", response_model=ReferenceToken)
async def subscribe(
    token: ReferenceToken = Depends(
        get_reference_token(TopicSubscriptionRequest)
    )
) -> ReferenceToken:
    """Generate a reference token to stream events."""
    return token


@router.get("/stream/{token}", response_model=Event)
async def stream(
    subscriptions: TopicSubscriptionRequest = Depends(get_cached_reference(TopicSubscriptionRequest)),
    bus: EventBus = Depends(get_event_bus),
) -> Event:
    """Submit a reference token to stream PI data. This is an event sourcing
    (SSE) endpoint.
    """
    subscriber = await bus.subscribe(subscriptions=subscriptions.subscriptions)
    send = iter_subscriber(subscriber)
    iterble = sse_handler(send, _LOGGER)
    return EventSourceResponse(iterble)


@router.websocket("/stream/{token}/ws")
async def stream_ws(
    websocket: WebSocket,
    _: BaseUser = Depends(
        requires(
            scopes=COMMANDCENTER_READ_ACCESS,
            any_=COMMANDCENTER_READ_ALLOW_ANY,
            raise_on_no_scopes=COMMANDCENTER_READ_RAISE_ON_NONE
        )
    ),
    subscriptions: TopicSubscriptionRequest = Depends(
        get_cached_reference(
            TopicSubscriptionRequest,
            raise_on_miss=False
        )
    ),
    bus: EventBus = Depends(get_event_bus),
) -> Event:
    """Submit a reference token to stream PI data over the websocket protocol."""
    try:
        if not subscriptions:
            await websocket.close(code=1008, reason="Invalid token")
            return
        try:
            subscriber = await bus.subscribe(subscriptions=subscriptions.subscriptions)
        except EventSubscriptionError:
            _LOGGER.warning("Refused connection due to a subscription error", exc_info=True)
            await websocket.close(code=1013)
            return
        except Exception:
            _LOGGER.error("Refused connection due to server error", exc_info=True)
            await websocket.close(code=1011)
            return
        else:
            await websocket.accept()
    except RuntimeError:
        # Websocket disconnected while we were subscribing
        subscriber.stop()
        return
    send = iter_subscriber(subscriber)
    await ws_handler(websocket, _LOGGER, None, send)


@router.get("/{topic}/last", response_model=Event)
async def last(event: Event = Depends(get_last_event)) -> Event:
    """Get last event for a topic-routing key combination."""
    return event


@router.get("/{topic}", response_model=EventQueryResult)
async def events(events: EventQueryResult = Depends(get_n_events)) -> EventQueryResult:
    """Get last n events for a topic-routing key combination."""
    return events