import asyncio
import base64
import inspect
import json
import threading
import logging
from collections.abc import Awaitable
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Tuple
from urllib.parse import parse_qs, urlsplit, urlunsplit, urlencode

from pydantic import BaseModel
from httpx import AsyncClient, Client
from httpx_auth.errors import (
    AuthenticationFailed,
    GrantNotProvided,
    InvalidGrantRequest,
    InvalidToken,
    TokenExpiryNotProvided
)



_LOGGER = logging.getLogger("commandcenter.http.oauth2")


class OAuthToken(BaseModel):
    token: str
    expiry: float


def add_parameters(initial_url: str, extra_parameters: dict) -> str:
    """Add parameters to an URL and return the new URL.

    Args:
        initial_url: The original url.
        extra_parameters: Dictionary of parameters name and value.
    
    Returns:
        url: The new URL containing parameters.
    """
    scheme, netloc, path, query_string, fragment = urlsplit(initial_url)
    query_params = parse_qs(query_string)
    query_params.update(
        {
            parameter_name: [parameter_value]
            for parameter_name, parameter_value in extra_parameters.items()
        }
    )

    new_query_string = urlencode(query_params, doseq=True)

    return urlunsplit((scheme, netloc, path, new_query_string, fragment))


def decode_base64(base64_encoded_string: str) -> str:
    """Decode base64, padding being optional.
    
    Args:
        base64_encoded_string: Base64 data as an ASCII byte string.
    Returns:
        string: The decoded byte string.
    """
    missing_padding = len(base64_encoded_string) % 4
    if missing_padding != 0:
        base64_encoded_string += "=" * (4 - missing_padding)
    return base64.b64decode(base64_encoded_string).decode("unicode_escape")


def is_expired(expiry: float, early_expiry: float) -> bool:
    return (
        datetime.utcfromtimestamp(expiry - early_expiry)
        < datetime.utcnow()
    )


def to_expiry(expires_in: int | str) -> float:
    expiry = datetime.utcnow().replace(
        tzinfo=timezone.utc
    ) + timedelta(seconds=int(expires_in))
    return expiry.timestamp()


def request_new_grant_with_post(
    client: Client | AsyncClient
) -> Callable[[Any], Tuple[str, int | None] | Awaitable[Tuple[str, int | None]]]:
    
    def wrapper(
        url: str,
        data,
        grant_name: str,
    ) -> Tuple[str, int]:
        response = client.post(url, data=data)

        if response.is_error:
            # As described in https://tools.ietf.org/html/rfc6749#section-5.2
            raise InvalidGrantRequest(response)

        content = response.json()
        token = content.get(grant_name)
        if not token:
            raise GrantNotProvided(grant_name, content)
        return token, content.get("expires_in")
    
    async def async_wrapper(
        url: str,
        data,
        grant_name: str,
    ) -> Tuple[str, int | None]:
        response = client.post(url, data=data)

        if response.is_error:
            # As described in https://tools.ietf.org/html/rfc6749#section-5.2
            raise InvalidGrantRequest(response)

        content = response.json()
        token = content.get(grant_name)
        if not token:
            raise GrantNotProvided(grant_name, content)
        return token, content.get("expires_in")

    if isinstance(client, AsyncClient):
        return async_wrapper
    return wrapper


class TokenMemoryCache:
    """
    Class to manage tokens using memory storage.
    """

    def __init__(self, sync: bool = True):
        self.tokens = {}
        self.forbid_concurrent_cache_access = threading.Lock() if sync else asyncio.Lock()
        self.forbid_concurrent_missing_token_function_call = threading.Lock() if sync else asyncio.Lock()

    def _add_bearer_token(self, key: str, token: str):
        """Set the bearer token and save it.

        Args:
            key: Key identifier of the token.
            token: Token value.
        
        Raises:
            InvalidToken: If the token is invalid.
            TokenExpiryNotProvided: If the expiry is not provided.
        """
        if not token:
            raise InvalidToken(token)

        header, body, other = token.split(".")
        body = json.loads(decode_base64(body))
        expiry = body.get("exp")
        if not expiry:
            raise TokenExpiryNotProvided(expiry)

        self._add_token(key, token, expiry)

    def _add_access_token(self, key: str, token: str, expires_in: int):
        """Set the bearer token and save it.
        
        Args:
            key: Key identifier of the token.
            token: Token value.
            expires_in: Number of seconds before token expiry.
        
        Raises:
            InvalidToken: If the token is invalid.
        """
        self._add_token(key, token, to_expiry(expires_in))

    def _add_token(self, key: str, token: str, expiry: float):
        """Set the bearer token and save it
        
        Args:
            key: Key identifier of the token.
            token: Token value.
            expiry: UTC timestamp of expiry.
        """
        with self.forbid_concurrent_cache_access:
            self.tokens[key] = token, expiry
            _LOGGER.debug(
                f"Inserting token expiring on {datetime.utcfromtimestamp(expiry)} "
                f"(UTC) with '{key}' key: {token}"
            )

    def get_token(
        self,
        key: str,
        *,
        early_expiry: float = 30.0,
        on_missing_token: Callable[[Any], Tuple[str, int | None] | Awaitable[Tuple[str, int | None]]] = None,
        **on_missing_token_kwargs,
    ) -> Callable[[], str | Awaitable[str]]:
        """Return the bearer token.

        Args:
            key: Key identifier of the token
            early_expiry: As the time between the token extraction from cache
                and the token reception on server side might not higher than
                one second, on slow networks, token might be expired when
                received by the actual server, even if still valid when fetched.
                This is the number of seconds to subtract to the actual token
                expiry. Token will be considered as expired 30 seconds before
                real expiry by default.
            on_missing_token: Function to call when token is expired or missing
                (returning token and expiry tuple)
            on_missing_token_kwargs: Arguments of the function (key-value arguments)
        
        Returns:
            token: The token
        
        Raises:
            AuthenticationFailed: If the token cannot be retrieved.
        """

        def check_cache() -> str | None:
            if key in self.tokens:
                bearer, expiry = self.tokens[key]
                if is_expired(expiry, early_expiry):
                    _LOGGER.debug(f"Authentication token with '{key}' key is expired.")
                    del self.tokens[key]
                else:
                    _LOGGER.debug(
                        "Using already received authentication, will expire on "
                        f"{datetime.utcfromtimestamp(expiry)} (UTC)."
                    )
                    return bearer
                
        def validate_token(new_token: str) -> str:
            if len(new_token) == 2:  # Bearer token
                state, token = new_token
                self._add_bearer_token(state, token)
            else:  # Access Token
                state, token, expires_in = new_token
                self._add_access_token(state, token, expires_in)
            if key != state:
                _LOGGER.warning(
                    "Using a token received on another key than expected. "
                    f"Expecting {key} but was {state}."
                )
            return state
        
        def validate_state(state: str) -> str | None:
            if state in self.tokens:
                bearer, expiry = self.tokens[state]
                _LOGGER.debug(
                    "Using newly received authentication, expiring on "
                    f"{datetime.utcfromtimestamp(expiry)} (UTC)."
                )
                return bearer
        
        def wrapper() -> str:
            _LOGGER.debug(f"Retrieving token with '{key}' key.")
            with self.forbid_concurrent_cache_access:
                bearer = check_cache()
                if bearer is not None:
                    return bearer

            _LOGGER.debug("Token cannot be found in cache.")
            if on_missing_token is not None:
                with self.forbid_concurrent_missing_token_function_call:
                    new_token = on_missing_token(**on_missing_token_kwargs)
                    state = validate_token(new_token)
                    
                with self.forbid_concurrent_cache_access:
                    bearer = validate_state(state)
                    if bearer is not None:
                        return bearer
            
            _LOGGER.debug(
                f"User was not authenticated: key {key} cannot be found in {self.tokens}."
            )
            raise AuthenticationFailed()
        
        async def async_wrapper() -> str:
            _LOGGER.debug(f"Retrieving token with '{key}' key.")
            async with self.forbid_concurrent_cache_access:
                bearer = check_cache()
                if bearer is not None:
                    return bearer

            _LOGGER.debug("Token cannot be found in cache.")
            if on_missing_token is not None:
                async with self.forbid_concurrent_missing_token_function_call:
                    new_token = await on_missing_token(**on_missing_token_kwargs)
                    state = validate_token(new_token)
                    
                async with self.forbid_concurrent_cache_access:
                    bearer = validate_state(state)
                    if bearer is not None:
                        return bearer
            
            _LOGGER.debug(
                f"User was not authenticated: key {key} cannot be found in {self.tokens}."
            )
            raise AuthenticationFailed()

        if inspect.iscoroutinefunction(on_missing_token):
            return async_wrapper
        return wrapper

    def clear(self):
        """Remove tokens from the cache."""
        with self.forbid_concurrent_cache_access:
            _LOGGER.debug("Clearing token cache.")
            self.tokens.clear()