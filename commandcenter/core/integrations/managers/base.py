import asyncio
import itertools
import logging
from contextlib import suppress
from typing import Any, List, Set, Type

from commandcenter.core.integrations.abc import (
    AbstractClient,
    AbstractManager,
    AbstractSubscriber
)
from commandcenter.core.integrations.exceptions import (
    ClientSubscriptionError,
    SubscriptionError
)



_LOGGER = logging.getLogger("commandcenter.core.integrations.managers")


class BaseManager(AbstractManager):
    """Base manager that implements common functionality across manager types."""
    def __init__(
        self,
        client: AbstractClient,
        subscriber: Type[AbstractSubscriber],
        max_subscribers: int = 100,
        maxlen: int = 100
    ) -> None:
        super().__init__(client, subscriber, max_subscribers, maxlen)
        
        self.exceptions: List[BaseException] = []
        self.failed: bool = False
        self.core_tasks: List[asyncio.Task] = []
        self.background_tasks: List[asyncio.Task] = []
        self.subscription_event: asyncio.Event = asyncio.Event()

    def subscriber_lost(self, subscriber: AbstractSubscriber) -> None:
        """Callback for subscriber instances after their `stop` method was called."""
        if not self._closed and not self._failed:
            assert subscriber in self.subscribers
            self.subscribers.remove(subscriber)
            self.subscription_event.set()
            _LOGGER.debug("Subscriber lost")
        else:
            assert not self.subscribers

    def _core_failed(self, fut: asyncio.Future) -> None:
        """Callback for core tasks if any task fails due to an unhandled exception."""
        self._failed = True
        exception = None
        
        with suppress(asyncio.CancelledError):
            exception = fut.exception()
        
        for t in self.core_tasks: t.cancel()
        self.core_tasks.clear()
        
        if exception is not None:
            self.exceptions.append(exception)
            _LOGGER.warning("Manager failed due to unhandled exception in core task", exc_info=exception)
        
        for subscriber in self.subscribers: subscriber.stop()
        self.subscribers.clear()

    def _task_complete(self, fut: asyncio.Future) -> None:
        """Callback for background tasks to be removed on completion."""
        try:
            self.background_tasks.remove(fut)
        except ValueError: # background tasks cleared, task not in list
            pass

    async def close(self) -> None:
        """Stop all subscribers and close the client."""
        for t in itertools.chain(self.core_tasks, self.background_tasks): t.cancel()
        self.core_tasks.clear()
        self.background_tasks.clear()
        await super().close()

    async def _subscribe(self, subscriptions: Set[Any]) -> None:
        """Subscribe to subscriptions on the client."""
        try:
            subscribed = await self.client.subscribe(subscriptions)
        except Exception as err:
            raise ClientSubscriptionError(err) from err
        if not subscribed:
            raise SubscriptionError()