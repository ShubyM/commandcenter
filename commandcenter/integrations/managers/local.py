import asyncio
import logging
from collections.abc import Sequence
from contextvars import Context
from typing import Type

import anyio

from commandcenter.integrations.base import BaseManager
from commandcenter.integrations.exceptions import (
    ClientClosed,
    ClientSubscriptionError,
    ManagerClosed,
    SubscriptionLimitError
)
from commandcenter.integrations.models import BaseSubscription
from commandcenter.integrations.protocols import Client, Subscriber



_LOGGER = logging.getLogger("commandcenter.integrations.managers")


class LocalManager(BaseManager):
    """A manager for non-distributed environments."""
    def __init__(
        self,
        client: Client,
        subscriber: Type[Subscriber],
        max_subscribers: int = 100,
        maxlen: int = 100
    ) -> None:
        super().__init__(client, subscriber, max_subscribers, maxlen)
        
        runner = Context().run(self._loop.create_task, self._run())
        runner.add_done_callback(lambda _: self._loop.create_task(self.close()))
        self._runner = runner

    @property
    def closed(self) -> bool:
        return self._runner is None or self._runner.done()

    async def close(self) -> None:
        fut = self._runner
        self._runner = None
        if fut is not None:
            fut.cancel()
        for fut in self._background: fut.cancel()
        await super().close()

    async def subscribe(
        self,
        subscriptions: Sequence[BaseSubscription]
    ) -> Subscriber:
        """Subscribe on the client and configure a subscriber.
        
        Args:
            subscriptions: The subscriptions to subscribe to.
        
        Returns:
            subscriber: The configured subscriber.
        
        Raises:
            ClientSubscriptionError: Unable to subscribe on the client.
            ManagerClosed: Cannot subscribe, manager is closed.
            SubscriptionLimitError: Max number of subscribers reached.
        """
        if self.closed:
            raise ManagerClosed()
        if len(self._subscribers) >= self._max_subscribers:
            raise SubscriptionLimitError(self._max_subscribers)
        
        subscriptions = set(subscriptions)
        
        try:
            subscribed = await self._client.subscribe(subscriptions)
        except ClientClosed as e:
            await self.close()
            raise ManagerClosed() from e

        if not subscribed:
            raise ClientSubscriptionError("Client refused subscriptions.")

        subscriber = self._subscriber()
        fut = subscriber.start(subscriptions, self._maxlen)
        fut.add_done_callback(self.subscriber_lost)
        self._subscribers[fut] = subscriber

        _LOGGER.debug("Added subscriber %i of %i", len(self._subscribers), self._max_subscribers)
        return subscriber

    def subscriber_lost(self, fut: asyncio.Future) -> None:
        super().subscriber_lost(fut)
        self._event.set()

    async def _get_client_messages(self) -> None:
        """Retrieve messages from the client and publish them to subscribers."""
        async for msg in self._client.messages():
            for fut, subscriber in self._subscribers.items():
                if not fut.done():
                    subscriber.publish(msg)

    async def _get_dropped_subscriptions(self) -> None:
        """Retrieve dropped subscriptions and stop any dependent subscribers."""
        async for msg in self._client.dropped():
            subscriptions = msg.subscriptions
            if subscriptions:
                for fut, subscriber in self._subscribers.items():
                    if (
                        not fut.done() and
                        subscriptions.difference(subscriber.subscriptions) != subscriptions
                    ):
                        fut.cancel()
                        if msg.error:
                            _LOGGER.warning(
                                "Subscriber dropped due to client connection error",
                                exc_info=msg.error
                            )
                        else:
                            # In the local manager we shouldnt have any dropped
                            # subscriber without an associated error on the
                            # client so this is a warning.
                            _LOGGER.warning("Subscriber dropped")

    async def _poll_subscriptions(self) -> None:
        """Poll subscribers and cross reference with any unusued client
        subscriptions.

        Unsubscribe from any client subscriptions which are no longer required.
        """
        while True:
            await self._event.wait()
            self._event.clear()
            
            # If a subscriber disconnects then reconnects we dont need to close
            # the client connections. So we give it a little time.
            await asyncio.sleep(5)
            
            subscriptions = self.subscriptions
            unsubscribe = self._client.subscriptions.difference(subscriptions)
            if unsubscribe:
                _LOGGER.debug("Unsubscribing from %i subscriptions", len(unsubscribe))
                fut = self._loop.create_task(self._client.unsubscribe(unsubscribe))
                fut.add_done_callback(self._background.discard)
                self._background.add(fut)

    async def _run(self) -> None:
        try:
            async with anyio.create_task_group() as tg:
                tg.start_soon(self._get_dropped_subscriptions)
                tg.start_soon(self._get_client_messages)
                tg.start_soon(self._poll_subscriptions)
        except Exception:
            _LOGGER.error("Manager failed", exc_info=True)
            raise