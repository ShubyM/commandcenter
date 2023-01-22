import asyncio
import logging
from collections.abc import AsyncIterable
from datetime import datetime
from typing import Callable, Dict, Set

from pydantic import ValidationError

from commandcenter.core.integrations.abc import AbstractSubscriber
from commandcenter.core.sources.pi_web.models import (
    PISubscriberMessage,
    PISubscription
)



_LOGGER = logging.getLogger("commandcenter.core.sources.pi_web")


class PISubscriber(AbstractSubscriber):
    """Subscriber implementation for the PI Web API data source."""
    def __init__(
        self,
        subscriptions: Set[PISubscription],
        callback: Callable[["PISubscriber"], None],
        maxlen: int
    ) -> None:
        super().__init__(subscriptions, callback, maxlen)
        self.chronological: Dict[str, datetime] = {
            subscription.web_id: None for subscription in subscriptions
        }
        self.data_waiter: asyncio.Future = None
        self.web_ids = {subscription.web_id for subscription in subscriptions}

    def publish(self, data: str) -> None:
        """Publish data to the subscriber. This method should only be called by
        the manager.
        """
        try:
            data = PISubscriberMessage.parse_raw(data)
        except ValidationError:
            _LOGGER.error("Message validation failed", exc_info=True, extra={"raw": data})

        self.data_queue.append(data)
        
        waiter = self.data_waiter
        self.data_waiter = None
        if waiter is not None and not waiter.done():
            waiter.set_result(None)

        _LOGGER.debug("Message published to subscriber")
    
    async def __aiter__(self) -> AsyncIterable[str]:
        """Async iterable for streaming real time PI data.
        
        This method is intended to be used in event sourcing and websocket contexts.
        The generator will stream data indefinitely until shutdown by the caller
        or stopped by the stream manager due to a subscription issue in the underlying
        client.

        Yields:
            data: A JSON string containing all the data updates for a single WebId.
        """
        # If `False`, `stop` called before caller could begin iterating
        if await self.start():
            stop = self.stop_waiter
            while not stop.done():
                if not self.data_queue:
                    waiter = self.loop.create_future()
                    self.data_waiter = waiter

                    await asyncio.wait([waiter, stop], return_when=asyncio.FIRST_COMPLETED)
                    if not waiter.done(): # `stop` called waiting for data
                        _LOGGER.debug("Subscriber stopped while waiting for data")
                        waiter.cancel()
                        self.data_waiter = None
                        break

                # Pop messages from the data queue until there are no messages
                # left
                while True:
                    try:
                        msg: PISubscriberMessage = self.data_queue.popleft()
                    except IndexError:
                        # Empty queue
                        break
                    # Each item represents all data points for a single WebId. We
                    # only yield data for a single WebId at a time
                    for item in msg.items:
                        web_id = item.web_id
                        timestamp = item.items[-1].timestamp # Most recent timestamp
                        if web_id in self.web_ids:
                            # Only yield the data if it is the next chronological
                            # item for that WebId. This ensures no duplicate data
                            # is sent
                            last_timestamp = self.chronological.get(web_id)
                            if last_timestamp is None or last_timestamp < timestamp:
                                yield item.json()
                            self.chronological[web_id] = timestamp