import logging
from collections.abc import Awaitable
from enum import IntEnum
from typing import Callable

import anyio
from fastapi import WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

from commandcenter.integrations.exceptions import DroppedSubscriber
from commandcenter.integrations.protocols import Subscriber



_LOGGER = logging.getLogger("commandcenter.streaming.ws")


async def ws_null_receive(websocket: WebSocket) -> None:
    """Reads messages from a websocket and does nothing with them."""
    while True:
        msg = await websocket.receive()
        if msg["type"] == "websocket.disconnect":
            code = msg["code"]
            if isinstance(code, IntEnum): # wsproto
                raise WebSocketDisconnect(code=code.value, reason=code.name)
            # websockets
            raise WebSocketDisconnect(code=code.code, reason=code.reason)


async def ws_subscriber_send(websocket: WebSocket, subscriber: Subscriber) -> None:
    """Iterates over a subscriber sending messages over the websocket connection."""
    with subscriber:
        async for msg in subscriber:
            await websocket.send_text(msg)
        else:
            assert subscriber.stopped
            raise DroppedSubscriber()


async def ws_handler(
    websocket: WebSocket,
    receive: Callable[[WebSocket], Awaitable[None]],
    send: Callable[[WebSocket], Awaitable[None]]
) -> None:
    """Manages a websocket connection."""
    try:
        async with anyio.create_task_group() as tg:
            tg.start_soon(receive(websocket))
            tg.start_soon(send(websocket))

    except WebSocketDisconnect as e:
        _LOGGER.debug(
            "Websocket connection closed by user",
            extra={
                "code": e.code,
                "reason": e.reason
            }
        )
    
    except Exception:
        _LOGGER.error("Connection closed abnormally", exc_info=True)
        try:
            await websocket.close(1006)
        except Exception:
            pass

    finally:
        if websocket.state != WebSocketState.DISCONNECTED:
            try:
                await websocket.close(1006)
            except:
                pass