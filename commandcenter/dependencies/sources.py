from fastapi import Request

from commandcenter.config.scopes import (
    CC_SCOPES_PIWEB_ACCESS,
    CC_SCOPES_PIWEB_ALLOW_ANY,
    CC_SCOPES_PIWEB_RAISE_ON_NONE,
    CC_SCOPES_TRAXX_ACCESS,
    CC_SCOPES_TRAXX_ALLOW_ANY,
    CC_SCOPES_TRAXX_RAISE_ON_NONE
)
from commandcenter.setup.sources import (
    setup_pi_http_client,
    setup_traxx_http_client
)
from commandcenter.auth import requires
from commandcenter.context import source_context
from commandcenter.sources import Sources
from commandcenter.sources.pi_web import PIWebAPI
from commandcenter.sources.traxx import TraxxAPI



SOURCE_REQUIRES = {
    Sources.PI_WEB_API: requires(
        scopes=list(CC_SCOPES_PIWEB_ACCESS),
        any_=CC_SCOPES_PIWEB_ALLOW_ANY,
        raise_on_no_scopes=CC_SCOPES_PIWEB_RAISE_ON_NONE
    ),
    Sources.TRAXX: requires(
        scopes=list(CC_SCOPES_TRAXX_ACCESS),
        any_=CC_SCOPES_TRAXX_ALLOW_ANY,
        raise_on_no_scopes=CC_SCOPES_TRAXX_RAISE_ON_NONE
    )
}


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


async def is_authorized(request: Request, source: Sources) -> None:
    """Verify user is authorized for access to a source."""
    await SOURCE_REQUIRES[source](request=request)