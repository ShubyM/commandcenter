import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple

from jose import ExpiredSignatureError, JWTError, jwt
from pydantic import BaseModel, SecretStr
from starlette.authentication import AuthenticationBackend, AuthCredentials
from starlette.requests import HTTPConnection

from commandcenter.auth.protocols import AuthenticationClient, JWTTokenHandler
from commandcenter.auth.user import BaseUser



_LOGGER = logging.getLogger("commandcenter.auth")


class TokenHandler(BaseModel, JWTTokenHandler):
    """Model for issuing and validating JWT's."""
    key: SecretStr
    expire: timedelta = 1800
    algorithm: str = "HS256"

    def issue(self, claims: Dict[str, Any]) -> str:
        to_encode = claims.copy()
        expire_at = datetime.utcnow() + self.expire
        to_encode.update({"exp": expire_at})
        
        return jwt.encode(
            to_encode,
            self.key.get_secret_value(),
            algorithm=self.algorithm
        )

    def validate(self, token: str) -> Optional[str]:
        try:
            payload = jwt.decode(
                token,
                self.key.get_secret_value(),
                algorithms=[self.algorithm]
            )
            return payload.get("sub")
        except ExpiredSignatureError:
            _LOGGER.debug("Token expired")
            return
        except JWTError:
            _LOGGER.debug("Received invalid token")
            return


class BaseAuthenticationBackend(AuthenticationBackend):
    """Standard interface for an authentication backend.
    
    Args:
        handler: A `TokenHandler` for issuing and validating tokens.
        client: An `AuthenticationClient` that queires an authentication/authorization
            database.
    """
    def __init__(self, handler: TokenHandler, client: AuthenticationClient) -> None:
        self.handler = handler
        self.client = client

    async def authenticate(self, conn: HTTPConnection) -> Optional[Tuple[AuthCredentials, BaseUser]]:
        """Validate a token from the connection and return a user along with
        their scopes
        
        Exceptions originating from the client *must* be caught and re-raised
        into an `AuthenticationError`.

        If a user is unauthenticated, this method *may* return `None` or some
        other unauthenticated user class.
        """
        raise NotImplementedError()