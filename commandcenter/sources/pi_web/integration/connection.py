import asyncio
import logging
from collections.abc import Iterable
from contextvars import Context
from typing import Set

from aiohttp import ClientSession, ClientWebSocketResponse
from pendulum.tz.zoneinfo.exceptions import InvalidTimezone
from pydantic import ValidationError

from commandcenter.integrations.base import BaseConnection
from commandcenter.sources.pi_web.models import (
    PIMessage,
    PISubscription,
    WebIdType
)
from commandcenter.util import EqualJitterBackoff



_LOGGER = logging.getLogger("commandcenter.sources.pi_web")



def build_url(web_id_type: WebIdType, subscriptions: Set[PISubscription]) -> str:
    """Build websocket '/streamset' url."""
    for subscription in subscriptions:
        if subscription.web_id_type != web_id_type:
            raise ValueError(f"Invalid WebIdType for subscription {subscription.web_id}")
    
    return (
        "/piwebapi/streamsets/channel?webId="
        f"{'&webId='.join([subscription.web_id for subscription in subscriptions])}"
        f"&webIdType={web_id_type.value}&includeInitialValues=True"
    )


async def create_connection(
    session: ClientSession,
    url: str,
    protocols: Iterable[str] | None,
    heartbeat: float,
    close_timeout: float,
    max_msg_size: int,
) -> ClientWebSocketResponse:
    """Open websocket connection to PI Web API."""
    return await session.ws_connect(
        url,
        protocols=protocols,
        heartbeat=heartbeat,
        timeout=close_timeout,
        max_msg_size=max_msg_size
    )


class PIWebConnection(BaseConnection):
    """Represents a single websocket connection to a PI Web API '/streamset'
    endpoint.
    """
    def __init__(self, max_subscriptions: int) -> None:
        super().__init__()
        self._max_subscriptions = max_subscriptions

    @property
    def capacity(self) -> int:
        """The number of additional subscriptions this connection can support."""
        return self._max_subscriptions - len(self._subscriptions)

    async def run(
        self,
        session: ClientSession,
        web_id_type: WebIdType,
        max_reconnect_attempts: int,
        initial_backoff: float,
        max_backoff: float,
        protocols: Iterable[str] | None,
        heartbeat: float,
        close_timeout: float,
        max_msg_size: int,
        timezone: str
    ) -> None:
        """Open a websocket connection to the PI Web API and process data
        indefinitely.
        """
        url = build_url(web_id_type=web_id_type, subscriptions=self._subscriptions)
        ws = await create_connection(
            session=session,
            url=url,
            protocols=protocols,
            heartbeat=heartbeat,
            close_timeout=close_timeout,
            max_msg_size=max_msg_size
        )
        backoff = EqualJitterBackoff(max_backoff, initial_backoff)
        _LOGGER.debug(
            "Established connection for %i subscriptions",
            len(self._subscriptions),
            extra={"url": url}
        )
        self._started.set()

        try:
            while True:
                async for msg in ws:
                    try:
                        data = PIMessage.parse_raw(msg.data)
                    except ValidationError:
                        _LOGGER.warning(
                            "Message validation failed",
                            exc_info=True,
                            extra={"raw": msg.data}
                        )
                    except Exception:
                        _LOGGER.error(
                            "An unhandled error occurred parsing the message",
                            exc_info=True,
                            extra={"raw": msg.data}
                        )
                    else:
                        try:
                            data.in_timezone(timezone)
                        except InvalidTimezone:
                            raise
                        await self._data.put(data.json())
                        self._total_published += 1
                        _LOGGER.debug("Published message on %s", self.__class__.__name__)
                else:
                    assert ws.closed
                    close_code = ws.close_code
                    _LOGGER.warning(
                        "Websocket closed by peer or network failure %i",
                        close_code
                    )
                    e = ws.exception()
                    if max_reconnect_attempts is not None and max_reconnect_attempts > 0:
                        attempts = 0
                        while attempts < max_reconnect_attempts:
                            _LOGGER.info(
                                "Attempting reconnect. Attempt %i of %i",
                                attempts + 1,
                                max_reconnect_attempts
                            )
                            try:
                                ws = await create_connection(
                                    session=session,
                                    url=url,
                                    protocols=protocols,
                                    heartbeat=heartbeat,
                                    close_timeout=close_timeout,
                                    max_msg_size=max_msg_size
                                )
                            except Exception:
                                backoff_delay = backoff.compute(attempts)
                                _LOGGER.debug(
                                    "Reconnect failed. Trying again in %0.2f seconds",
                                    backoff_delay,
                                    exc_info=True
                                )
                                await asyncio.sleep(backoff_delay)
                                attempts += 1
                            else:
                                _LOGGER.info("Connection re-established")
                                break
                        if ws.closed:
                            if e is not None:
                                raise e
                            break
                    else:
                        if e is not None:
                            raise e
                        break
        finally:
            self._started.clear()
            if not ws.closed:
                await ws.close()
    
    async def start(
        self,
        subscriptions: Set[PISubscription],
        data: asyncio.Queue,
        session: ClientSession,
        web_id_type: WebIdType,
        max_reconnect_attempts: int,
        initial_backoff: float,
        max_backoff: float,
        protocols: Iterable[str] | None,
        heartbeat: float,
        close_timeout: float,
        max_msg_size: int,
        timezone: str
    ) -> asyncio.Future:
        self._subscriptions.update(subscriptions)
        self._data = data
        runner = Context().run(
            self._loop.create_task,
            self.run(
                session=session,
                web_id_type=web_id_type,
                max_reconnect_attempts=max_reconnect_attempts,
                initial_backoff=initial_backoff,
                max_backoff=max_backoff,
                protocols=protocols,
                heartbeat=heartbeat,
                close_timeout=close_timeout,
                max_msg_size=max_msg_size,
                timezone=timezone
            )
        )
        waiter = self._loop.create_task(self._started.wait())
        try:
            await asyncio.wait([runner, waiter], return_when=asyncio.FIRST_COMPLETED)
        except asyncio.CancelledError:
            runner.cancel()
            waiter.cancel()
            raise
        if not waiter.done():
            e = runner.exception()
            if e is not None:
                raise e
            raise RuntimeError("Runner exited without throwing exception.")
        return runner