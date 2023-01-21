import asyncio
import functools
import logging
import random
from datetime import datetime, timedelta
from typing import Callable, List, Sequence, Set, cast

import pendulum
from aiohttp import ClientSession
from pydantic import ValidationError

from commandcenter.core.integrations.abc import AbstractClient, AbstractConnection
from commandcenter.core.integrations.util import TIMEZONE
from commandcenter.core.sources.traxx.exceptions import TraxxExpiredSession
from commandcenter.core.sources.traxx.http.client import TraxxClient
from commandcenter.core.sources.traxx.http.data.util import handle_request
from commandcenter.core.sources.traxx.models import TraxxSensorMessage, TraxxSubscription



_LOGGER = logging.getLogger("commandcenter.core.sources.traxx")


class TraxxConnection(AbstractConnection):
    """Represents an HTTP interface to the a Traxx sensor endpoint.
    
    This connection uses basic HTTP/1.1 to retrieve a CSV file for the the desginated
    sensor in a loop.

    `TraxxConnection` and HTTP connections are not 1:1, a `ClientSession` pool
    still maintains the total number of I/O connections.
    """
    def __init__(
        self,
        callback: Callable[["TraxxConnection"], None],
        *,
        update_interval: float,
        max_missed_updates: int,
        backoff_factor: float,
        initial_backoff: float,
        timezone: str
    ) -> None:
        super().__init__(callback)
        self.update_interval = update_interval
        self.max_missed_updates = max_missed_updates
        self.backoff_factor = backoff_factor
        self.initial_backoff = initial_backoff
        self.timezone = timezone

        self.client: TraxxClient = None

    @property
    def subscription(self) -> TraxxSubscription:
        return list(self.subscriptions)[0]

    async def start(
        self,
        subscriptions: Set[TraxxSubscription],
        feeder: asyncio.Queue,
        client: TraxxClient
    ) -> None:
        """Start the websocket connection and background data retrieval task."""
        await super().start(subscriptions, feeder)
        self.client = client

        runner = self.loop.create_task(self.run())
        runner.add_done_callback(self.connection_lost)
        self.runner = runner

    async def run(self) -> None:
        subscription = self.subscription
        asset_id = subscription.asset_id
        sensor_id = subscription.sensor_id
        last_update: datetime = None
        last_timestamp: datetime = None
        attempts = 0

        while True:
            now = datetime.now()
            if last_update is None:
                last_update = now - timedelta(minutes=15)
            # ensure start is sufficiently far back that we get data most
            # of the time
            start_time = min(last_update, now-timedelta(minutes=2))
            begin = int(pendulum.instance(start_time, self.timezone).float_timestamp * 1000)
            end = int(pendulum.instance(now, self.timezone).float_timestamp * 1000)
            try:
                reader = await handle_request(
                    self.client.sensors.sensor_data(
                        asset_id,
                        sensor_id,
                        begin,
                        end,
                        tz=self.timezone
                    )
                )
            except TraxxExpiredSession:
                _LOGGER.warning("Failed to retireve sensor data, session expired.")
                raise
            except Exception:
                _LOGGER.warning("Failed to retrieve sensor data: %r", subscription, exc_info=True)
                if attempts >= self.max_missed_updates:
                    raise
                backoff_delay = (
                    self.initial_backoff * self.backoff_factor ** attempts
                )
                backoff_delay = min(
                    
                    self.update_interval,
                    int(backoff_delay)
                )
                _LOGGER.info(
                    "Attempting next update in %0.2f. Attempt %i of %i",
                    backoff_delay,
                    attempts + 1,
                    self.max_missed_updates
                )
                await asyncio.sleep(backoff_delay)
                attempts += 1
                continue
            else:
                last_update = now
            
            sleeper = self.loop.create_task(
                asyncio.sleep(self.update_interval + random.randint(-2000, 2000)/1000)
            )
            if reader is None:
                _LOGGER.debug("No content returned for sensor: %r", subscription)
            else:
                items = [{"timestamp": line[0], "value": line[1]} for line in reader]
                if items and self.online:
                    try:
                        data = TraxxSensorMessage(
                            asset_id=asset_id,
                            sensor_id=sensor_id,
                            items=items
                        )
                    except ValidationError:
                        _LOGGER.warning("Message validation failed", exc_info=True, extra={"raw": items})
                    else:
                        if last_timestamp is None:
                            last_timestamp = data.items[-1].timestamp
                        else:
                            data.filter(last_timestamp)
                            if data.items:
                                last_timestamp = data.items[-1].timestamp
                                data.in_timezone(self.timezone)
                                await self.feeder.put(data.json())
            await sleeper


class TraxxStreamClient(AbstractClient):
    """Client implementation for 'real-time' Traxx data."""
    def __init__(
        self,
        session: ClientSession,
        *,
        max_subscriptions: int = 100,
        max_buffered_messages: int = 1000,
        update_interval: float = 30.0,
        max_missed_updates: int = 10,
        backoff_factor: float = 1.618,
        initial_backoff: float = 5.0,
        timezone: str = TIMEZONE
    ) -> None:
        super().__init__(max_buffered_messages)
        self.client = TraxxClient(session)
        self.max_subscriptions = max_subscriptions
        self.connection_factory = functools.partial(
            TraxxConnection,
            self.connection_lost,
            update_interval=update_interval,
            max_missed_updates=max_missed_updates,
            backoff_factor=backoff_factor,
            initial_backoff=initial_backoff,
            timezone=timezone
        )

    @property
    def capacity(self) -> int:
        return self.max_subscriptions - len(self.subscriptions)

    @property
    def closed(self) -> bool:
        """`True` if the underlying session is closed."""
        return self.client.session.closed

    async def close(self) -> None:
        for connection in self.connections: connection.stop()
        await self.client.close()

    async def subscribe(self, subscriptions: Sequence[TraxxSubscription]) -> bool:
        """Subscribe the client to a sequence of Traxx sensors.
        Args:
            subscriptions: The sequence of subscriptions to subscribe to
        Returns:
            result: A boolean indicating the success of the operation. `True` means
                that all subscriptions were successfully subscribed to. `False` means
                that none of the subscriptions were subscribed to
        """
        subscriptions: Set[TraxxSubscription] = set(subscriptions)
        if self.capacity >= len(subscriptions):
            subscriptions = subscriptions.difference(self.subscriptions)
            connections = [self.connection_factory() for _ in range(len(subscriptions))]
            for subscription, connection in zip(subscriptions, connections):
                await connection.start({subscription}, self.data_queue, self.client)
                connection.toggle()
            self.connections.extend(connections)
            return True
        return False

    async def unsubscribe(self, subscriptions: Sequence[TraxxSubscription]) -> bool:
        """Unsubscribe the client from a sequence of Traxx sensors.
        Args:
            subscriptions: The sequence of subscriptions to subscribe to
        Returns:
            result: A boolean indicating the success of the operation. `True` means
                that all subscriptions were successfully subscribed to. `False` means
                that none of the subscriptions were subscribed to
        """
        subscriptions: Set[TraxxSubscription] = set(subscriptions)
        dne = subscriptions.difference(self.subscriptions)
        subscriptions = subscriptions.difference(dne)

        connections = cast(List[TraxxConnection], self.connections)
        if subscriptions:
            for subscription in subscriptions:
                for connection in connections:
                    if connection.subscription == subscription:
                        connection.stop()
        return True