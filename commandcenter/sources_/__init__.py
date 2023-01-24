from contextlib import contextmanager
from typing import Any, Dict, Generator, Tuple, Type

from commandcenter.core.sources import AvailableSources
from commandcenter.core.integrations.abc import (
    AbstractClient,
    AbstractManager,
    AbstractSubscriber
)
from commandcenter.core.sources import AvailableSources
from commandcenter.core.util.context import source_context
from .pi_web import get_pi_http_client, resolve_pi_channel_client_dependencies
from .traxx import get_traxx_http_client, resolve_traxx_stream_client_dependencies



__all__ = [
    "get_pi_http_client",
    "get_traxx_http_client",
    "set_source",
    "build_client_for_manager"
]


@contextmanager
def set_source(source: AvailableSources) -> Generator[None, None, None]:
    """Context manager which sets the source context."""
    token = source_context.set(source)
    try:
        yield
    finally:
        source_context.reset(token)


def build_client_for_manager(
    source: str,
    manager: AbstractManager
) -> Tuple[AbstractClient, Type[AbstractSubscriber], Dict[str, Any]]:
    """Resolve the source context to a client type and configure the client
    per the environment configuration.
    """
    subscriber = None
    if source == AvailableSources.PI_WEB_API:
        from commandcenter.core.sources.pi_web import PISubscriber
        subscriber = PISubscriber
        client, args, kwargs, add_kwargs = resolve_pi_channel_client_dependencies(manager)
    elif source == AvailableSources.TRAXX:
        from commandcenter.core.sources.traxx import TraxxSubscriber
        subscriber = TraxxSubscriber
        client, args, kwargs, add_kwargs = resolve_traxx_stream_client_dependencies(manager)
    elif source is None:
        raise RuntimeError(
            "Source context not set. Ensure context dependency is added to path "
            "definition."
        )
    else:
        raise RuntimeError("Received invalid source.")

    return client(*args, **kwargs), subscriber, add_kwargs