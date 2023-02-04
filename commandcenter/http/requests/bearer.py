import hashlib
from typing import Tuple

from httpx import Client
from requests import Request
from requests.auth import AuthBase

from commandcenter.http.oauth2 import (
    TokenMemoryCache,
    add_parameters,
    request_new_grant_with_post
)



class OAuth2ResourceOwnerPasswordCredentials(AuthBase):
    """Implements the OAuth2ResourceOwnerPasswordCredentials flow.

    This is ported from [httpx-auth](https://pypi.org/project/httpx-auth/#resource-owner-password-credentials-flow)
    and has been adapted to work with `aiohttp`.

    Args:
        token_url: OAuth 2 token URL.
        username: Resource owner user name.
        password: Resource owner password.
        timeout: Maximum amount of seconds to wait for a token to be received
            once requested. Wait for 1 minute by default.
        header_name: Name of the header field used to send token. Token will be
            sent in Authorization header field by default.
        header_value: Format used to send the token value. "{token}" must be
            present as it will be replaced by the actual token. Token will be
            sent as "Bearer {token}" by default.
        scope: Scope parameter sent to token URL as body. Can also be a list of
            scopes. Not sent by default.
        token_field_name: Field name containing the token. access_token by
            default.
        early_expiry: Number of seconds before actual token expiry where token
            will be considered as expired. Default to 30 seconds to ensure
            token will not expire between the time of retrieval and the time
            the request. reaches the actual server. Set it to 0 to deactivate
            this feature and use the same token until actual expiry.
        client: httpx.AsyncClient instance that will be used to request the token.
            Use it to provide a custom proxying rule for instance.
        kwargs: all additional authorization parameters that should be put as
            body parameters in the token URL.
    """
    def __init__(self, token_url: str, username: str, password: str, **kwargs):
        self.token_url = token_url
        if not self.token_url:
            raise ValueError("Token URL is mandatory.")
        self.username = username
        if not self.username:
            raise ValueError("User name is mandatory.")
        self.password = password
        if not self.password:
            raise ValueError("Password is mandatory.")

        self.header_name = kwargs.pop("header_name", None) or "Authorization"
        self.header_value = kwargs.pop("header_value", None) or "Bearer {token}"
        if "{token}" not in self.header_value:
            raise ValueError("header_value parameter must contains {token}.")

        self.token_field_name = kwargs.pop("token_field_name", None) or "access_token"
        self.early_expiry = float(kwargs.pop("early_expiry", None) or 30.0)

        # Time is expressed in seconds
        self.timeout = int(kwargs.pop("timeout", None) or 60)
        self.client = kwargs.pop("client", None)

        # As described in https://tools.ietf.org/html/rfc6749#section-4.3.2
        self.data = {
            "grant_type": "password",
            "username": self.username,
            "password": self.password,
        }
        scope = kwargs.pop("scope", None)
        if scope:
            self.data["scope"] = " ".join(scope) if isinstance(scope, list) else scope
        self.data.update(kwargs)

        all_parameters_in_url = add_parameters(self.token_url, self.data)
        self.state = hashlib.sha512(all_parameters_in_url.encode("unicode_escape")).hexdigest()

        self.token_cache = TokenMemoryCache(sync=True)

    def request_new_token(self) -> Tuple[str, str | None, int]:
        client = self.client or Client()
        self.configure_client(client)
        try:
            # As described in https://tools.ietf.org/html/rfc6749#section-4.3.3
            call = request_new_grant_with_post(client)
            token, expires_in = call(
                self.token_url,
                self.data,
                self.token_field_name
            )
        finally:
            # Close client only if it was created by this module
            if self.client is None:
                client.close()
        # Handle both Access and Bearer tokens
        return (self.state, token, expires_in) if expires_in else (self.state, token)

    def configure_client(self, client: Client) -> None:
        client.auth = (self.username, self.password)
        client.timeout = self.timeout

    def __call__(self, request: Request):
        call = self.token_cache.get_token(
            self.state,
            early_expiry=self.early_expiry,
            on_missing_token=self.request_new_token,
        )
        token = call()
        request.headers[self.header_name] = self.header_value.format(token=token)
        return request