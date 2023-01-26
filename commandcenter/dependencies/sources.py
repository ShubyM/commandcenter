from commandcenter.context import source_context
from commandcenter.setup.sources import (
    setup_pi_http_client,
    setup_traxx_http_client
)
from commandcenter.sources import Sources
from commandcenter.sources.pi_web import PIWebAPI
from commandcenter.sources.traxx import TraxxAPI



async def get_pi_http_client() -> PIWebAPI:
    """Dependency for retrieving a PI Web HTTP client."""
    return setup_pi_http_client()


async def get_traxx_http_client() -> TraxxAPI:
    """Dependency for retrieving a Traxx HTTP client."""
    return setup_traxx_http_client()


class source:
    """Dependency which sets the data source for a route.
    
    This is critical in ensuring the `get_manager` dependency works correctly.
    """
    def __init__(self, source: Sources):
        self.source = source

    async def __call__(self) -> None:
        try:
            token = source_context.set(self.source)
            yield
        finally:
            source_context.reset(token)