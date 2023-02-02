import asyncio

import anyio
from aiormq import Connection, Channel

from commandcenter.eventbus.models import Topic
from commandcenter.integrations.base import BaseClient, BaseConnection
from commandcenter.integrations.models import Subscription



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
        exchange: str
    ) -> None:
        channel = await connection.channel(publisher_confirms=False)
        try:
            await channel.exchange_declare(exchange=exchange, exchange_type="direct")
            declare_ok = await channel.queue_declare(exclusive=True)
            binds = [
                channel.queue_bind(declare_ok.queue, exchange, topic.name)
                for topic in self._subscriptions
            ]
            await asyncio.gather(*binds)
            confirm.set_result(None)
        finally:
            if not channel.is_closed:
                await channel.close()

    async def start(
        self,
        subscriptions: Set[Subscription],
        data: asyncio.Queue,
        connection: Connection,
        exchange: str
    ) -> asyncio.Future:
        return await super().start(subscriptions, data, *args, **kwargs)
    
    async def _run(self, channel: Channel)
    
    async def _wait_for_connection_lost(self, fut: asyncio.Future) -> None:
        await fut