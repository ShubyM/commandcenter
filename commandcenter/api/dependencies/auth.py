from fastapi import Depends
from fastapi.security import OAuth2PasswordBearer

from commandcenter.api.setup.auth import setup_auth_backend
from commandcenter.auth import AuthenticationClient, TokenHandler



async def get_auth_client() -> AuthenticationClient:
    """Dependency for retrieving an authentication client."""
    return setup_auth_backend().client


async def get_token_handler() -> TokenHandler:
    """Dependency for retrieving a token handler."""
    return setup_auth_backend().handler


scheme = OAuth2PasswordBearer("/users/token", auto_error=False)

async def enable_interactive_auth(_: str | None = Depends(scheme)) -> None:
    """Dependency that enables authorization in the interactive docs. Debug only."""