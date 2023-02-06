from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm

from commandcenter.auth import Token
from commandcenter.auth.base import TokenHandler
from commandcenter.auth.protocols import AuthenticationClient
from commandcenter.api.dependencies import (
    get_auth_client,
    get_token_handler
)



router = APIRouter(prefix="/users", tags=["Users"])


@router.post("/token", response_model=Token)
async def token(
    form: OAuth2PasswordRequestForm = Depends(),
    handler: TokenHandler = Depends(get_token_handler),
    client: AuthenticationClient = Depends(get_auth_client)
) -> Token:
    """Retrieve an access token for the API."""
    authenticated = await client.authenticate(form.username, form.password)
    if authenticated:
        claims = {"sub": form.username}
        access_token = handler.issue(claims=claims)
        return Token(access_token=access_token, token_type="bearer")
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Incorrect username or password"
    )