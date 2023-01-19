import logging
from typing import Optional, Tuple

from bonsai.errors import LDAPError
from fastapi.security.utils import get_authorization_scheme_param
from starlette.authentication import AuthCredentials, AuthenticationError
from starlette.requests import HTTPConnection

from commandcenter.core.auth.abc import AbstractAuthenticationBackend
from commandcenter.core.auth.backends.ad.client import ActiveDirectoryClient
from commandcenter.core.auth.backends.ad.models import ActiveDirectoryUser
from commandcenter.core.auth.token import TokenHandler



_LOGGER = logging.getLogger("commandcenter.core.auth.ad")


class ActiveDirectoryBackend(AbstractAuthenticationBackend):
    """Active Directory backend for starlette `AuthenticationMiddleware`
    
    This backend assumes a bearer token exists in the Authorization header. The
    token provides the username which is then queried against the active directory
    server.
    
    If no token exists or the token is invalid/expired, an unauthenticated user
    is returned and downstream authorization at endpoints will fail. If we are
    unable to communicate with the AD server, an `AuthenticationError` is raised.
    """
    def __init__(self, handler: TokenHandler, client: ActiveDirectoryClient) -> None:
        super().__init__(handler, client)

    async def authenticate(self, conn: HTTPConnection) -> Optional[Tuple[AuthCredentials, ActiveDirectoryUser]]:
        """Extract bearer token from authorization header and retrieve user entry
        from AD.

        Raises:
            AuthenticationError: An error occurred during the AD lookup.
        """
        authorization = conn.headers.get("Authorization")
        if not authorization:
            return
        
        scheme, token = get_authorization_scheme_param(authorization)
        if scheme.lower() != "bearer":
            return
        
        username = self.handler.get_username(token)
        if username is None:
            return
        
        for _ in range(len(self.client.dcs)):
            try:
                user = await self.client.get_user(username)
            except LDAPError:
                self.client.rotate()
                _LOGGER.error("Unable to retrieve user information from active directory", exc_info=True)
                continue
            except Exception as err:
                _LOGGER.error("An unhandled error occurred during authentication", exc_info=True)
                raise AuthenticationError("An unhandled error occurred.") from err
            else:
                return AuthCredentials(user.scopes), user
        else:
            raise AuthenticationError("Unable to communicate with authentication backend.")