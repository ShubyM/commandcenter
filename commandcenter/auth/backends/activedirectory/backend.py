import logging
from typing import Optional, Tuple, cast

from bonsai.errors import LDAPError
from fastapi.security.utils import get_authorization_scheme_param
from starlette.authentication import AuthCredentials, AuthenticationError
from starlette.requests import HTTPConnection

from commandcenter.auth.base import BaseAuthenticationBackend
from commandcenter.auth.backends.activedirectory.client import ActiveDirectoryClient
from commandcenter.auth.backends.activedirectory.user import ActiveDirectoryUser



_LOGGER = logging.getLogger("commandcenter.auth.activedirectory")


class ActiveDirectoryBackend(BaseAuthenticationBackend):
    """Active Directory backend for starlette's `AuthenticationMiddleware`
    
    This backend assumes a bearer token exists in the authorization header. The
    token provides the username which is then queried against the active directory
    server.
    
    If no token exists or the token is invalid/expired, an unauthenticated user
    is returned. If we cannot communicate with the AD server, an
    `AuthenticationError` is raised.
    """
    async def authenticate(
        self,
        conn: HTTPConnection
    ) -> Optional[Tuple[AuthCredentials, ActiveDirectoryUser]]:
        """Extract bearer token from authorization header and retrieve user entry
        from active directory.

        Raises:
            AuthenticationError: An error occurred during the AD lookup.
        """
        client = cast(ActiveDirectoryClient, self.client)

        authorization = conn.headers.get("Authorization")
        if not authorization:
            return
        
        scheme, token = get_authorization_scheme_param(authorization)
        if scheme.lower() != "bearer":
            return
        
        username = self.handler.validate(token)
        if username is None:
            return
        
        for _ in range(len(self.client.dcs)):
            try:
                user = await self.client.get_user(username)
            except LDAPError:
                self.client.rotate()
                _LOGGER.warning("Rotating client", exc_info=True)
                continue
            except Exception as err:
                _LOGGER.error("An unhandled error occurred", exc_info=True)
                raise AuthenticationError("An unhandled error occurred.") from err
            else:
                return AuthCredentials(user.scopes), user
        else:
            # All domain controller servers are unreachable
            raise AuthenticationError(
                "Unable to communicate with active directory server."
            )