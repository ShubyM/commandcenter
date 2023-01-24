import asyncio
import logging
from collections.abc import Sequence
from typing import Type

import anyio

from commandcenter.integrations.base import BaseManager
from commandcenter.integrations.exceptions import (
    ClientClosed,
    ClientSubscriptionError,
    ManagerClosed,
    SubscriptionError,
    SubscriptionLimitError
)
from commandcenter.integrations.models import BaseSubscription
from commandcenter.integrations.protocols import Client, Subscriber
from commandcenter.util.enums import ObjSelection



_LOGGER = logging.getLogger("commandcenter.integrations.managers")


class LocalManager(BaseManager):
    """A manager designed for single process environments."""
    def __init__(
        self,
        client: Client,
        subscriber: Type[Subscriber],
        max_subscribers: int = 100,
        maxlen: int = 100
    ) -> None:
        super().__init__(client, subscriber, max_subscribers, maxlen)
        fut = self.loop.create_task(self.start())
        self.background.add(fut)
        fut.add_done_callback(self.background.discard)

    async def subscribe(
        self,
        subscriptions: Sequence[BaseSubscription]
    ) -> Subscriber:
        """Subscribe to the subscriptions on the client instance and configure
        a subscriber.
        
        Args:
            subscriptions: A sequence of subscriptions to subscribe to.
        
        Returns:
            subscriber: Async iterator for receiving incoming data from the
                data source.
        
        Raises:
            ClientSubscriptionError: An unhandled error occurred subscribing on
                the client.
            ManagerClosed: Cannot subscribe, manager is closed.
            SubscriptionError: Unable to subscribe on the client.
            SubscriptionLimitError: Max number of subscribers reached.
        """
        if self.closed:
            raise ManagerClosed(self.errors)
        if len(self.subscribers) >= self.max_subscribers:
            raise SubscriptionLimitError(self.max_subscribers)

        await self.ready.wait()
        
        subscriptions = set(subscriptions)
        
        try:
            subscribed = await self.client.subscribe(subscriptions)
        except Exception as e:
            raise ClientSubscriptionError(e) from e
        if not subscribed:
            raise SubscriptionError()

        subscriber = self.subscriber()
        fut = self.loop.create_task(
            subscriber.start(
                subscriptions=subscriptions,
                maxlen=self.maxlen
            )
        )
        fut.add_done_callback(self.subscriber_lost)
        self.subscribers[fut] = subscriber

        _LOGGER.debug("Added subscriber %i of %i", len(self.subscribers), self.max_subscribers)
        
        return subscriber

    def subscriber_lost(self, fut: asyncio.Future) -> None:
        super().subscriber_lost(fut)
        self.subscriber_event.set()

    async def _data(self) -> None:
        """Core task to retrieve data from client and publish it to subscribers."""
        async for msg in self.client.messages():
            for fut, subscriber in self.subscribers.items():
                if not fut.done():
                    subscriber.publish(msg)

    async def _errors(self) -> None:
        """Retreive errors from the client.
        
        If a connection error affects a subscriber, the subscriber will be
        stopped.
        """
        async for e in self.client.errors():
            subscriptions = e.subscriptions
            for fut, subscriber in self.subscribers.items():
                if subscriptions.difference(subscriber.subscriptions) != subscriptions:
                    fut.cancel()
                    _LOGGER.warning(
                        "Subscriber stopped due to client connection error",
                        exc_info=e.error
                    )

    async def _poll(self) -> None:
        """Poll manager subscriptions after a subscriber is lost.
        
        Unsubscribe on the client if a subscription is no longer needed.
        """
        while True:
            await self.subscriber_event.wait()
            try:
                subscriptions = self.subscriptions
                unubscribe = self.client.subscriptions.difference(subscriptions)
                if unubscribe:
                    _LOGGER.debug("Unsubscribing from %i subscriptions", len(unubscribe))
                    fut = self.loop.create_task(self.client.unsubscribe(unubscribe))
                    self.background.add(fut)
                    fut.add_done_callback(self.background.discard)
            finally:
                self.subscriber_event.clear()

    async def start(self) -> None:
        """Start data processing tasks in the background."""
        while True:
            try:
                async with anyio.create_task_group() as tg:
                    tg.start_soon(self._data())
                    tg.start_soon(self._errors())
                    tg.start_soon(self._poll())
                    self.ready.set()
            except ClientClosed:
                self.loop.create_task(self.close())
                break
            except Exception:
                _LOGGER.error("Unhandled error in manager", exc_info=True)
            finally:
                self.ready.clear()


class Managers(ObjSelection):
    DEFAULT = "default", LocalManager
    LOCAL = "local", LocalManager