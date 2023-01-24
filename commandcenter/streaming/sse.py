import logging
from collections.abc import AsyncIterable
from typing import Any

from commandcenter.integrations.exceptions import DroppedSubscriber
from commandcenter.integrations.protocols import Subscriber



_LOGGER = logging.getLogger("commandcenter.streaming.sse")


async def sse_subscriber_send(subscriber: Subscriber) -> AsyncIterable[str]:
    """Iterates over a subscriber yielding events."""
    with subscriber:
        async for msg in subscriber:
            yield msg
        else:
            assert subscriber.stopped
            raise DroppedSubscriber()


async def sse_handler(send: AsyncIterable[Any]) -> AsyncIterable[Any]:
    """Wraps an async iterable, yields events."""
    try:
        async for msg in send:
            yield msg
    except Exception:
        _LOGGER.error("Connection closed abnormally", exc_info=True)
        raise