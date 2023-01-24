import asyncio
import logging
from datetime import timedelta
from typing import Set

from commandcenter.integrations.models import BaseSubscription



class BaseCollection:
    def __init__(
        self,
        subscriptions: Set[BaseSubscription],
        delta: timedelta | float
    ) -> None:
        self.subscriptions = subscriptions
        if isinstance(delta, int):
            delta = timedelta(seconds=delta)
        if not isinstance(delta, timedelta):
            raise TypeError(f"Expected type 'timedelta | int', got {type(delta)}")
        self.delta = delta

        loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        self.close_waiter: asyncio.Future = loop.create_future()

    @property
    def closed(self) -> bool:
        return self.close_waiter is not None and not self.close_waiter.done()

    async def close(self) -> None:
        waiter = self.close_waiter
        self.close_waiter = None
        if waiter is not None and not waiter.done():
            waiter.set_result(None)