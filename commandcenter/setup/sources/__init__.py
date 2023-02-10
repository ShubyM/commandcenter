from typing import Any, Dict, Tuple, Type

from commandcenter.integrations import Client, Manager, Subscriber
from commandcenter.sources import Sources
from .pi_web import resolve_pi_web_client_dependencies, setup_pi_http_client
from .traxx import resolve_traxx_client_dependencies, setup_traxx_http_client



__all__ = [
    "setup_pi_http_client",
    "setup_traxx_http_client",
    "setup_client"
]


def setup_client(
    source: str,
    manager: Manager
) -> Tuple[Client, Type[Subscriber], Dict[str, Any]]:
    """Resolve the source context to a client type and configure the client.
    
    This must be run in the same thread as the event loop.
    """
    subscriber = None
    if source == Sources.PI_WEB_API:
        from commandcenter.sources.pi_web import PIWebSubscriber
        subscriber = PIWebSubscriber
        client, args, kwargs, add_kwargs = resolve_pi_web_client_dependencies(manager)
    elif source == Sources.TRAXX:
        from commandcenter.sources.traxx import TraxxSubscriber
        subscriber = TraxxSubscriber
        client, args, kwargs, add_kwargs = resolve_traxx_client_dependencies(manager)
    elif source is None:
        raise RuntimeError(
            "Source context not set. Ensure context dependency is added to path "
            "definition."
        )
    else:
        raise RuntimeError("Received invalid source.")

    return client(*args, **kwargs), subscriber, add_kwargs