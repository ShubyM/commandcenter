import logging
import secrets
from datetime import datetime, timedelta
from typing import Dict, Optional

from jose import ExpiredSignatureError, JWTError, jwt
from pydantic import BaseModel, SecretStr



_LOGGER = logging.getLogger("commandcenter.core.auth")


class TokenHandler(BaseModel):
    """Model for issuing, verifying, and revoking JWT's."""
    key: SecretStr
    expire: float = 30.0
    algorithm: str = "HS256"

    def create_token(self, claims: Dict) -> str:
        """Issue a JWT.
        
        Args:
            claims: dictionary of JWT claims.
        
        Raises:
            JWTError: If there was an error encoding the claims.
        """
        to_encode = claims.copy()
        expire_at = datetime.utcnow() + timedelta(minutes=self.expire)
        to_encode.update({"exp": expire_at})
        
        return jwt.encode(
            to_encode,
            self.key.get_secret_value(),
            algorithm=self.algorithm
        )

    def get_username(self, token: str) -> Optional[str]:
        """Get the username of the JWT issued.

        Args:
            token: The JWT provided by the client.
        Returns:
            username: If the token is invalid or expired this returns `None`.
        """
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
            _LOGGER.debug("Received invalid token", exc_info=True)
            return

    def invalidate(self) -> None:
        """Invalidate all issued tokens by changing the secret key.
        
        This allows an adminstrator to remotely invalidate all JWT's via an
        API endpoint.
        """
        self.key._secret_value = secrets.token_hex(32)