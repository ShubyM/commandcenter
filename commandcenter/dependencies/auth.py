from commandcenter.auth.base import TokenHandler
from commandcenter.auth.protocols import AuthenticationClient
from commandcenter.setup.auth import setup_auth_backend



async def get_auth_client() -> AuthenticationClient:
    """Dependency for retrieving an authentication client."""
    return setup_auth_backend().client


async def get_token_handler() -> TokenHandler:
    """Dependency for retrieving a token handler."""
    return setup_auth_backend().handler