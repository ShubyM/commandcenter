from collections.abc import AsyncIterable
from typing import Sequence, TypeVar

from commandcenter.core.integrations.abc import AbstractManager
from commandcenter.core.integrations.models import BaseSubscription



T = TypeVar("T")


async def subscriber_event_generator(
    manager: AbstractManager,
    subscriptions: Sequence[BaseSubscription]
) -> AsyncIterable[T]:
    with await manager.subscribe(subscriptions) as subscriber:
        async for msg in subscriber:
            yield msg