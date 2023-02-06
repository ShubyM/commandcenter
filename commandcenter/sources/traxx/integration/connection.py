import asyncio
import logging
import random
from contextvars import Context
from datetime import datetime, timedelta
from typing import Set

import pendulum
from pydantic import ValidationError

from commandcenter.integrations.base import BaseConnection
from commandcenter.sources.traxx.data.client import TraxxAPI
from commandcenter.sources.traxx.data.util import handle_request
from commandcenter.sources.traxx.exceptions import TraxxExpiredSession
from commandcenter.sources.traxx.models import TraxxSensorMessage, TraxxSubscription
from commandcenter.util import EqualJitterBackoff



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
        self._started.set()
        try:
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
                                    self._total_published += 1
                await sleeper
        finally:
            self._started.clear()

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
        runner = Context().run(
            self._loop.create_task,
            self.run(
                client=client,
                update_interval=update_interval,
                max_missed_updates=max_missed_updates,
                initial_backoff=initial_backoff,
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