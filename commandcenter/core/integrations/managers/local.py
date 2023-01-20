import logging
from typing import Sequence

from commandcenter.core.integrations.abc import AbstractSubscriber
from commandcenter.core.integrations.exceptions import (
    FailedManager,
    SubscriptionLimitError
)
from commandcenter.core.integrations.managers.base import BaseManager
from commandcenter.core.integrations.models import BaseSubscription



_LOGGER = logging.getLogger("commandcenter.core.integrations.managers")


class LocalManager(BaseManager):
    """A manager designed for non-distributed environments.
    
    The local manager does not lock any subscriptions in a distributed environment.
    Two subscribers on two `LocalManager` instances can subscribe to the same
    subscriptions and two seperate connections would be opened up in each process
    to the data source. The local manager should only be used in development or
    single instance deployments.
    """
    def _start(self) -> None:
        """Start core tasks on manager."""
        if self.core_tasks:
            return
        coros = [
            self._retrieve_data(),
            self._retrieve_errors(),
            self._poll_required_subscriptions()
        ]
        tasks = [self.loop.create_task(coro) for coro in coros]
        [t.add_done_callback(self._core_failed) for t in tasks]
        self.core_tasks.extend(tasks)

    async def subscribe(
        self,
        subscriptions: Sequence[BaseSubscription]
    ) -> AbstractSubscriber:
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
            FailedManager: Cannot subscribe due to an unhandled exception on the
                manager.
            SubscriptionError: Unable to subscribe on the client.
            SubscriptionLimitError: Max number of subscribers reached.
        """
        if self.failed:
            assert self.exceptions
            raise FailedManager(self.exceptions)
        self._start()
        if len(self.subscribers) >= self.max_subscribers:
            raise SubscriptionLimitError(self.max_subscribers)
        subscriptions = set(subscriptions)
        await self._subscribe(subscriptions)
        subscriber = self.subscriber(
            subscriptions,
            self.subscriber_lost,
            self.maxlen,
        )
        self.subscribers.append(subscriber)
        _LOGGER.debug("Added subscriber %i of %i", len(self.subscribers), self.max_subscribers)
        return subscriber

    async def _retrieve_data(self) -> None:
        """Core task to retrieve data from client and publish it to subscribers."""
        async for msg in self.client.messages():
            for subscriber in self.subscribers: subscriber.publish(msg)

    async def _retrieve_errors(self) -> None:
        """Core task to retrieve connection errors from the client.
        
        If a connection error affects a subscriber, the subscriber will be
        stopped.
        """
        async for err in self.client.errors():
            subscriptions = err.subscriptions
            for subscriber in self.subscribers:
                if subscriptions.difference(subscriber.subscriptions) != subscriptions:
                    subscriber.stop()
                    _LOGGER.warning(
                        "Subscriber stopped due to client connection error",
                        exc_info=err.exc
                    )

    async def _poll_required_subscriptions(self) -> None:
        """Poll subscriptions for all subscribers and unsubscribe on the client
        if a subscription is no longer needed.
        """
        while True:
            await self.subscription_event.wait()
            try:
                subscriptions = self.subscriptions
                # Check required subscriptions (from subscribers) against subscriptions
                # on the client. Any subscription on the client not required by
                # a subscriber can be unsubscribed from
                unubscribe = self.client.subscriptions.difference(subscriptions)
                if unubscribe:
                    _LOGGER.debug("Unsubscribing from %i subscriptions", len(unubscribe))
                    # We dont want to block this coroutine so we unsubscribe in
                    # the background
                    t = self.loop.create_task(self.client.unsubscribe(unubscribe))
                    t.add_done_callback(self._task_complete)
                    self.background_tasks.append(t)
            finally:
                self.subscription_event.clear()