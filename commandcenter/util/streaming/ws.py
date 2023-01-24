import logging
from enum import IntEnum

import anyio
from fastapi import WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

from commandcenter.integrations.exceptions import DroppedSubscriber
from commandcenter.integrations.protocols import Subscriber



_LOGGER = logging.getLogger("commandcenter.util.streaming.ws")


async def _ws_receive(websocket: WebSocket) -> None:
    while True:
        msg = await websocket.receive()
        if msg["type"] == "websocket.disconnect":
            code = msg["code"]
            if isinstance(code, IntEnum): # wsproto
                raise WebSocketDisconnect(code=code.value, reason=code.name)
            # websockets
            raise WebSocketDisconnect(code=code.code, reason=code.reason)


async def _ws_send(websocket: WebSocket, subscriber: Subscriber) -> None:
    with subscriber:
        async for msg in subscriber:
            await websocket.send_text(msg)
        else:
            assert subscriber.stopped
            raise DroppedSubscriber()


async def subscriber_ws_handler(websocket: WebSocket, subscriber: Subscriber) -> None:
    """Manages a websocket connection and sends messages from a subscriber."""
    try:
        async with anyio.create_task_group() as tg:
            tg.start_soon(_ws_receive(websocket))
            tg.start_soon(_ws_send(websocket, subscriber))

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
                await websocket.close()
            except:
                pass