from abc import ABC, abstractmethod
from typing import Optional, Tuple

from starlette.authentication import AuthCredentials, AuthenticationBackend
from starlette.requests import HTTPConnection

from commandcenter.core.auth.models import BaseUser
from commandcenter.core.auth.token import TokenHandler



class AbstractAuthenticationClient(ABC):
    """Standard interface for a client that communicates with an
    authentication/authorization database.
    """
    @abstractmethod
    async def authenticate(self, username: str, password: str) -> bool:
        """Authenticate a username and password against an authority.
        
        Args:
            username: Username.
            password: Password.
        
        Returns:
            authenticated: `True` if valid user credentials, `False` otherwise.
        """
    
    @abstractmethod
    async def get_user(self, identifier: str) -> BaseUser:
        """Retrieve user information from an authority.
        
        Args:
            identifier: Unique identifier for user.
        
        Returns:
            user: `BaseUser` with populated user information.
        """


class AbstractAuthenticationBackend(ABC, AuthenticationBackend):
    """Standard interface for an authentication backend.
    
    Args:
        handler: A `TokenHandler` for issuing and verifying tokens.
        client: A client that communicates with an authentication/authorization
            database to retrieve user information.
    """
    def __init__(self, handler: TokenHandler, client: AbstractAuthenticationClient) -> None:
        self.handler = handler
        self.client = client

    @abstractmethod
    async def authenticate(self, conn: HTTPConnection) -> Optional[Tuple[AuthCredentials, BaseUser]]:
        """Validate JWT from connection and retrieve user information.
        
        Exceptions originating from the client should be caught and re-raised
        into an `AuthenticationError`.

        If a user is unauthenticated, this method can return `None` or some
        other unauthenticated user class. Endpoints downstream requiring
        authorization can then raise a 401 error.
        """