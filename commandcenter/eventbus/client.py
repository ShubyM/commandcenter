import asyncio

from aiormq import Connection, Channel

from commandcenter.eventbus.models import Topic
from commandcenter.integrations.base import BaseClient, BaseConnection



class EventBusConnection(BaseConnection):

    @property
    def topic(self) -> Topic:
        try:
            return list(self._subscriptions)[0]
        except IndexError:
            return

    async def run(
        self,
        confirm: asyncio.Future,
        connection: Connection,
        exchange: str,
        
    ) -> None:
        return await super().run(confirm, *args, **kwargs)

    async def start(
        self,
        subscriptions: Set[Subscription],
        data: asyncio.Queue,
        *args: Any,
        **kwargs: Any
    ) -> asyncio.Future:
        return await super().start(subscriptions, data, *args, **kwargs)