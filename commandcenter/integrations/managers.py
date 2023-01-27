import asyncio
import logging
from collections.abc import Sequence
from typing import Type

import anyio

from commandcenter.exceptions import (
    ClientClosed,
    ClientError,
    ManagerClosed,
    SubscriptionLimitError
)
from commandcenter.integrations.base import BaseManager
from commandcenter.integrations.models import BaseSubscription
from commandcenter.integrations.protocols import Client, Subscriber
from commandcenter.util import ObjSelection



_LOGGER = logging.getLogger("commandcenter.integrations.managers")


class LocalManager(BaseManager):
    """A manager intended for single process environments."""
    def __init__(
        self,
        client: Client,
        subscriber: Type[Subscriber],
        max_subscribers: int = 100,
        maxlen: int = 100
    ) -> None:
        super().__init__(client, subscriber, max_subscribers, maxlen)
        self._runner: asyncio.Task = self._loop.create_task(self._run())

    @property
    def closed(self) -> bool:
        return self._runner is None or self._runner.done()

    async def close(self) -> None:
        fut = self._runner
        self._runner = None
        fut.cancel()
        await super().close()

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
            raise ManagerClosed()
        if len(self._subscribers) >= self._max_subscribers:
            raise SubscriptionLimitError(self._max_subscribers)
        
        subscriptions = set(subscriptions)
        
        try:
            self._client.subscribe(subscriptions)
        except SubscriptionLimitError:
            raise
        except ClientClosed:
            await self.close()
            raise ManagerClosed()
        except Exception as e:
            raise ClientError(e) from e

        subscriber = self._subscriber()
        fut = self._loop.create_task(
            subscriber.start(
                subscriptions=subscriptions,
                maxlen=self._maxlen
            )
        )
        fut.add_done_callback(self.subscriber_lost)
        self._subscribers[fut] = subscriber

        _LOGGER.debug("Added subscriber %i of %i", len(self._subscribers), self._max_subscribers)
        await subscriber.started.wait()
        return subscriber

    def subscriber_lost(self, fut: asyncio.Future) -> None:
        super().subscriber_lost(fut)
        self._event.set()

    async def _messages(self) -> None:
        """Retrieve messages from client and publish it to subscribers."""
        async for msg in self._client.messages():
            for fut, subscriber in self._subscribers.items():
                if not fut.done():
                    subscriber.publish(msg)

    async def _dropped(self) -> None:
        """Retrieve dropped subscriptions and stop any dependent subscribers."""
        async for msg in self._client.dropped():
            subscriptions = msg.subscriptions
            if subscriptions:
                for fut, subscriber in self._subscribers.items():
                    if subscriptions.difference(subscriber.subscriptions) != subscriptions:
                        fut.cancel()
                        # Theoretically there should always be an error associated
                        # if we get here
                        if msg.error:
                            _LOGGER.warning(
                                "Subscriber dropped due to client connection error",
                                exc_info=msg.error
                            )
                        # If there isnt, then thats a bug...
                        else:
                            _LOGGER.error("Subscriber dropped unexpectedly")

    async def _poll(self) -> None:
        """Poll subscribers and cross reference with any unusued client
        subscriptions.

        Unsubscribe from any client subscriptions which are no longer required.
        """
        while True:
            await self._event.wait()
            # If a subscriber disconnects then reconnects we dont need to close
            # the client connections. So we give it a little time.
            await asyncio.sleep(5)
            try:
                subscriptions = self.subscriptions
                unubscribe = self._client.subscriptions.difference(subscriptions)
                if unubscribe:
                    _LOGGER.debug("Unsubscribing from %i subscriptions", len(unubscribe))
                    self._client.unsubscribe(unubscribe)
            finally:
                self._event.clear()

    async def _run(self) -> None:
        """Manages background tasks on manager."""
        while True:
            try:
                async with anyio.create_task_group() as tg:
                    tg.start_soon(LocalManager._dropped, self)
                    tg.start_soon(LocalManager._messages, self)
                    tg.start_soon(LocalManager._poll, self)
            except ClientClosed:
                self._loop.create_task(self.close())
                break
            except Exception:
                _LOGGER.error("Unhandled error in manager", exc_info=True)
                # This really shouldnt happen so we are going to break here.
                break


class Managers(ObjSelection):
    DEFAULT = "default", LocalManager
    LOCAL = "local", LocalManager