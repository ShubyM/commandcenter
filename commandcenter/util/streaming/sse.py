import logging
from collections.abc import AsyncIterable

from commandcenter.integrations.exceptions import DroppedSubscriber
from commandcenter.integrations.protocols import Subscriber



_LOGGER = logging.getLogger("commandcenter.util.streaming.sse")


async def subscriber_sse_handler(subscriber: Subscriber) -> AsyncIterable[str]:
    """Iterates over a subscriber yielding events."""
    try:
        with subscriber:
            async for msg in subscriber:
                yield msg
            else:
                assert subscriber.stopped
                raise DroppedSubscriber()
    except Exception:
        _LOGGER.error("Connection closed abnormally", exc_info=True)
        raise