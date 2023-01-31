import asyncio
import functools
import logging
import random
from collections.abc import Awaitable
from contextvars import Context
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Set

import pendulum
from aiohttp import ClientSession
from pydantic import ValidationError

from commandcenter.integrations.base import BaseClient, BaseConnection
from commandcenter.integrations.exceptions import ClientClosed
from commandcenter.sources.traxx.api.client import TraxxAPI
from commandcenter.sources.traxx.api.util import handle_request
from commandcenter.sources.traxx.exceptions import TraxxExpiredSession
from commandcenter.sources.traxx.models import TraxxSensorMessage, TraxxSubscription
from commandcenter.util import TIMEZONE, EqualJitterBackoff



_LOGGER = logging.getLogger("commandcenter.sources.traxx")


class TraxxConnection(BaseConnection):
    """Represents an HTTP interface to a Traxx sensor endpoint.
    
    This connection uses basic HTTP/1.1 to retrieve a CSV file for the the desginated
    sensor in a loop.

    `TraxxConnection` and HTTP connections are not 1:1, a `ClientSession` pool
    still maintains the total number of TCP connections.
    """
    @property
    def subscription(self) -> TraxxSubscription | None:
        try:
            return list(self._subscriptions)[0]
        except IndexError:
            return

    async def run(
        self,
        confirm: asyncio.Future,
        client: TraxxAPI,
        update_interval: float,
        max_missed_updates: int,
        initial_backoff: float,
        timezone: str
    ) -> None:
        """Query Traxx at set intervals and process data indefinitely."""
        subscription = self.subscription
        asset_id = subscription.asset_id
        sensor_id = subscription.sensor_id
        
        last_update: datetime = None
        last_timestamp: datetime = None
        attempts = 0

        backoff = EqualJitterBackoff(cap=update_interval, initial=initial_backoff)
        if confirm.done():
            raise RuntimeError("Confirm finished but task was not cancelled.")
        confirm.set_result(None)
        while True:
            now = datetime.now()
            start_time = min(last_update or now, now-timedelta(minutes=15))
            begin = int(pendulum.instance(start_time, timezone).float_timestamp * 1000)
            end = int(pendulum.instance(now, timezone).float_timestamp * 1000)
            try:
                reader = await handle_request(
                    client.sensors.sensor_data(
                        asset_id,
                        sensor_id,
                        begin,
                        end,
                        tz=timezone
                    )
                )
            except TraxxExpiredSession:
                _LOGGER.warning("Failed to retireve sensor data, session expired.")
                raise
            except Exception:
                _LOGGER.warning("Failed to retrieve sensor data: %r", subscription, exc_info=True)
                if attempts >= max_missed_updates:
                    raise
                backoff_delay = backoff.compute(attempts)
                _LOGGER.info(
                    "Attempting next update in %0.2f. Attempt %i of %i",
                    backoff_delay,
                    attempts + 1,
                    max_missed_updates
                )
                await asyncio.sleep(backoff_delay)
                attempts += 1
                continue
            else:
                last_update = now
            
            # If data queue is buffer is full and we have to wait, we dont
            # want to add more time on top of that so we start the timer now
            sleeper = self._loop.create_task(
                asyncio.sleep(update_interval + random.randint(-1000, 1000)/1000)
            )
            if reader is None:
                _LOGGER.debug("No content returned for sensor: %r", subscription)
            else:
                items = [{"timestamp": line[0], "value": line[1]} for line in reader]
                if items:
                    try:
                        data = TraxxSensorMessage(
                            subscription=subscription,
                            items=items
                        )
                    except ValidationError:
                        _LOGGER.warning(
                            "Message validation failed",
                            exc_info=True,
                            extra={"raw": items}
                        )
                    else:
                        if last_timestamp is None:
                            last_timestamp = data.items[-1].timestamp
                        else:
                            data.filter(last_timestamp)
                            if data.items:
                                last_timestamp = data.items[-1].timestamp
                                data.in_timezone(timezone)
                                await self._data.put(data.json())
            await sleeper

    async def start(
        self,
        subscriptions: Set[TraxxSubscription],
        data: asyncio.Queue,
        client: TraxxAPI,
        update_interval: float,
        max_missed_updates: int,
        initial_backoff: float,
        timezone: str
    ) -> None:
        self._subscriptions.update(subscriptions)
        self._data = data
        confirm = self._loop.create_future()
        runner = Context().run(
            self._loop.create_task,
            self.run(
                confirm,
                client=client,
                update_interval=update_interval,
                max_missed_updates=max_missed_updates,
                initial_backoff=initial_backoff,
                timezone=timezone
            )
        )
        try:
            await asyncio.wait([runner, confirm], return_when=asyncio.FIRST_COMPLETED)
        except asyncio.CancelledError:
            runner.cancel()
            confirm.cancel()
            raise
        if not confirm.done():
            e = runner.exception()
            if e is not None:
                raise e
            raise RuntimeError("Runner exited without throwing exception.")
        return runner


class TraxxClient(BaseClient):
    """Client implementation for 'real-time' Traxx data."""
    def __init__(
        self,
        session: ClientSession,
        *,
        max_subscriptions: int = 100,
        max_buffered_messages: int = 1000,
        update_interval: float = 30,
        max_missed_updates: int = 10,
        initial_backoff: float = 10,
        timezone: str = TIMEZONE
    ) -> None:
        super().__init__(max_buffered_messages)

        client = TraxxAPI(session)
        self._start_connection: Callable[
            ["TraxxConnection", Set[TraxxSubscription], asyncio.Queue],
            Awaitable[None]
        ] = functools.partial(
            TraxxConnection.start,
            client=client,
            update_interval=update_interval,
            max_missed_updates=max_missed_updates,
            initial_backoff=initial_backoff,
            timezone=timezone
        )

        self._client = client
        self._max_subscriptions = max_subscriptions
        self._lock: asyncio.Lock = asyncio.Lock()

    @property
    def capacity(self) -> int:
        return self._max_subscriptions - len(self.subscriptions)

    @property
    def closed(self) -> bool:
        return self._client.session.closed

    async def close(self) -> None:
        for fut, _ in self._connections.items(): fut.cancel()
        if not self._client.session.closed:
            await self._client.close()

    async def subscribe(self, subscriptions: Set[TraxxSubscription]) -> None:
        if self.closed:
            raise ClientClosed()
        
        async with self._lock:
            subscriptions = list(subscriptions.difference(self.subscriptions))
            capacity = self.capacity
            count = len(subscriptions)
            
            if subscriptions and capacity >= count:
                connections = await self._create_connections(subscriptions)
                if connections is None:
                    return False
                self._connections.update(connections)
            elif subscriptions and capacity < count:
                return False
            return True

    async def unsubscribe(self, subscriptions: Set[TraxxSubscription]) -> bool:
        if self.closed:
            raise ClientClosed()

        async with self._lock:
            not_applicable = subscriptions.difference(self.subscriptions)
            subscriptions = subscriptions.difference(not_applicable)

            if subscriptions:
                for subscription in subscriptions:
                    for fut, connection in self._connections.items():
                        if connection.subscription == subscription:
                            fut.cancel()

    async def _create_connections(
        self,
        subscriptions: Set[TraxxSubscription]
    ) -> Dict[asyncio.Future, TraxxConnection] | None:
        """Create connections to support all subscriptions."""
        connections: List[TraxxConnection] = []
        starters: List[Awaitable[asyncio.Future]] = []
        for subscription in subscriptions:
                connection = TraxxConnection()
                starters.append(
                    self._start_connection(
                        connection,
                        set([subscription]),
                        self._data
                    )
                )
                connections.append(connection)
        
        results = await asyncio.gather(*starters, return_exceptions=True)
        if any([isinstance(result, Exception) for result in results]):
            for result in results:
                if isinstance(result, asyncio.Future):
                    result.cancel()
                elif isinstance(result, Exception):
                    _LOGGER.warning("Connection failed to start", exc_info=result)
            return None

        for fut in results:
            fut.add_done_callback(self.connection_lost)

        return {fut: connection for fut, connection in zip(results, connections)}