from typing import Any, Dict, Protocol

from commandcenter.auth.user import BaseUser



class AuthenticationClient(Protocol):
    async def authenticate(self, username: str, password: str) -> bool:
        """Authenticate a username and password against an authority.
        
        Args:
            username: Username.
            password: Password.
        
        Returns:
            authenticated: `True` if valid user credentials, `False` otherwise.
        """
        ...
    
    async def get_user(self, username: str) -> BaseUser:
        """Retrieve user information from an authority.
        
        Args:
            username: Username.
        
        Returns:
            user: `BaseUser` with populated user information.
        """
        ...


class JWTTokenHandler(Protocol):
    def issue(self, claims: Dict[str, Any]) -> str:
        """Issue a JWT.
        
        Args:
            claims: dictionary of JWT claims.
        
        Raises:
            JWTError: If there was an error encoding the claims.
        """
        ...

    def validate(self, token: str) -> str | None:
        """Validate and extract the username from the token.

        Args:
            token: JWT from the client.
        Returns:
            username: If the token is invalid or expired this returns `None`.
        """
        ...