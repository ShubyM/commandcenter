import asyncio
import functools
import logging
from collections.abc import Awaitable
from typing import Callable, Dict, List, Set

from aiohttp import ClientSession

from commandcenter.integrations.base import BaseClient
from commandcenter.integrations.exceptions import ClientClosed
from commandcenter.sources.traxx.data.client import TraxxAPI
from commandcenter.sources.traxx.integration.connection import TraxxConnection
from commandcenter.sources.traxx.models import TraxxSubscription
from commandcenter.util import TIMEZONE



_LOGGER = logging.getLogger("commandcenter.sources.traxx")


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