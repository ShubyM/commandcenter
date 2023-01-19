import asyncio
import logging
from collections.abc import AsyncIterable
from typing import Callable, Set

from pydantic import ValidationError

from commandcenter.core.integrations.abc import AbstractSubscriber
from commandcenter.core.sources.traxx.models import (
    TraxxSubscriberMessage,
    TraxxSubscription
)



_LOGGER = logging.getLogger("commandcenter.core.sources.traxx")


class TraxxSubscriber(AbstractSubscriber):
    def __init__(
        self,
        subscriptions: Set[TraxxSubscription],
        callback: Callable[["TraxxSubscriber"], None],
        maxlen: int,
    ) -> None:
        super().__init__(subscriptions, callback, maxlen)
        self._data_waiter: asyncio.Future = None
        self._sensors = [
            f"{subscription.sensor_id}-{subscription.asset_id}"
            for subscription in subscriptions
        ]

    def publish(self, data: str) -> None:
        """Publish data to the subscriber. This method should only be called by
        the manager.
        """
        try:
            data = TraxxSubscriberMessage.parse_raw(data)
        except ValidationError:
            _LOGGER.error("Message validation failed", exc_info=True, extra={"raw": data})

        super().publish(data)
        
        waiter = self._data_waiter
        self._data_waiter = None
        if waiter is not None and not waiter.done():
            waiter.set_result(None)

        _LOGGER.debug("Message published to subscriber")
    
    async def __aiter__(self) -> AsyncIterable[TraxxSubscriberMessage]:
        """Async iterable for streaming real time Traxx data.
        
        This method is intended to be used in event sourcing and websocket contexts.
        The generator will stream data indefinitely until shutdown by the caller
        or stopped by the stream manager due to a subscription issue in the underlying
        client.
        Yields:
            data: A BaseModel containing all the data updates for a single sensor.
        """
        # If `False`, `stop` called before caller could begin iterating
        if not self._stopped:
            if self._stop_waiter is not None:
                raise RuntimeError("Two coroutines cannot iterate over a subscriber simultaneously.")
            loop = asyncio.get_running_loop()
            stop = loop.create_future()
            self._loop = loop
            self._stop_waiter = stop
            # Loop forever until `stop` is called by stream manager
            while not stop.done():
                if not self._data_queue:
                    waiter = self._loop.create_future()
                    self._data_waiter = waiter

                    await asyncio.wait([waiter, stop], return_when=asyncio.FIRST_COMPLETED)
                    if not waiter.done(): # `stop` called waiting for data
                        _LOGGER.debug("Subscriber stopped while waiting for data")
                        waiter.cancel()
                        self._data_waiter = None
                        break

                # Pop messages from the data queue until there are no messages
                # left
                while True:
                    try:
                        msg: TraxxSubscriberMessage = self._data_queue.popleft()
                    except IndexError:
                        # Empty queue
                        break
                    if f"{msg.sensor_id}-{msg.asset_id}" in self._sensors:
                        # The traxx messages are guarenteed to be in monotonically
                        # increasingly order so we dont need to sort or filter
                        # the data here
                        yield msg