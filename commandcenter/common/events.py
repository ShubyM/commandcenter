import csv
import functools
import io
import logging
from collections.abc import AsyncIterable, AsyncIterator
from typing import Callable, List, Tuple

import anyio
from fastapi import WebSocket
from jsonlines import Writer
from pydantic import BaseModel
from starlette.websockets import WebSocketState

from commandcenter.core.integrations.abc import AbstractSubscriber
from commandcenter.core.integrations.exceptions import DroppedSubscriber
from commandcenter.core.integrations.models import BaseSubscriptionRequest
from commandcenter.core.integrations.types import JSONPrimitive, TimeseriesRow
from commandcenter.core.timeseries.collection import Flush



_LOGGER = logging.getLogger("commandcenter.common.events")


async def subscriber_event_generator(subscriber: AbstractSubscriber) -> AsyncIterable[str]:
    """Iterates over a subscriber yielding events."""
    with subscriber:
        try:
            async for msg in subscriber:
                yield msg
            else:
                assert subscriber.stopped, "Subscriber iteration ended but it is not stopped."
                _LOGGER.info("Subscriber dropped by manager.")
        except anyio.get_cancelled_exc_class():
            raise
        except Exception:
            _LOGGER.error("Unhandled error in subscriber", exc_info=True)
            raise


async def websocket_event_generator(websocket: WebSocket, subscriber: AbstractSubscriber) -> None:
    """Manages a websocket connection and sends messages from a subscriber."""
    try:
        async with anyio.create_task_group() as tg:
            
            async def wrap_receive() -> None:
                while True:
                    try:
                        msg = await websocket.receive()
                        if msg["type"] == "websocket.disconnect":
                            _LOGGER.debug(
                                "Websocket connection closed by user",
                                extra={
                                    "code": msg["code"].value,
                                    "reason": msg["code"].name
                                }
                            )
                            break
                    except anyio.get_cancelled_exc_class():
                        raise
                    finally:
                        if not tg.cancel_scope.cancel_called:
                            tg.cancel_scope.cancel()

            async def wrap_send() -> None:
                with subscriber:
                    try:
                        async for msg in subscriber:
                            await websocket.send_text(msg)
                        else:
                            assert subscriber.stopped, "Subscriber iteration ended but it is not stopped."
                            raise DroppedSubscriber("Subscriber dropped by manager.")
                    except (anyio.get_cancelled_exc_class(), DroppedSubscriber):
                        raise
                    finally:
                        if not tg.cancel_scope.cancel_called:
                            tg.cancel_scope.cancel()

            tg.start_soon(wrap_receive)
            tg.start_soon(wrap_send)
    
    except DroppedSubscriber:
        _LOGGER.warning("Websocket connection closed due to dropped subscriber")
        try:
            await websocket.close(1006)
        except Exception:
            pass
    
    except Exception:
        _LOGGER.error("Websocket connection closed abnormally", exc_info=True)
        try:
            await websocket.close(1006)
        except Exception:
            pass

    finally:
        # Last resort in case we really exited abnormally
        if websocket.state != WebSocketState.DISCONNECTED:
            try:
                await websocket.close()
            except Exception:
                pass


def jsonlines_write() -> Tuple[Writer, io.StringIO]:
    buffer = io.StringIO()
    writer = Writer(buffer)
    return functools.partial(writer.write), buffer


def csv_write() -> csv.writer:
    buffer = io.StringIO(newline='')
    writer = csv.writer(buffer, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    return functools.partial(writer.writerow), buffer


async def timeseries_chunk_event_generator(
    iterator: AsyncIterator[TimeseriesRow],
    buffer: io.StringIO,
    writer: Callable[[List[JSONPrimitive]], None],
    chunk_size: int = 1000
) -> AsyncIterable[str]:
    count = 0
    while True:
        try:
            timestamp, row = await iterator.__anext__()
        except StopAsyncIteration:
            chunk = buffer.getvalue()
            if chunk:
                yield chunk
            return
        except Exception:
            _LOGGER.warning("Chunk generator failed iterating over contents", exc_info=True)
            raise
        count += 1
        if timestamp is Flush:
            chunk = buffer.getvalue()
            if chunk:
                yield chunk
            buffer.seek(0)
            buffer.truncate(0)
            count = 0

        writer([timestamp.isoformat(), *row])
        
        if count >= chunk_size:
            chunk = buffer.getvalue()
            if chunk:
                yield chunk
            buffer.seek(0)
            buffer.truncate(0)
            count = 0
            
