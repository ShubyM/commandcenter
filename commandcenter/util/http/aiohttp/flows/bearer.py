import hashlib
import json
from collections.abc import AsyncGenerator
from datetime import datetime
from typing import Tuple

from aiohttp import ClientRequest, ClientResponse, hdrs
from aiohttp.log import client_logger
from aiohttp.connector import Connection
from httpx import AsyncClient
from starlette.datastructures import Secret

from commandcenter.util.http.aiohttp.auth import AuthFlow
from commandcenter.util.http.oauth2 import (
    OAuthToken,
    TokenMemoryCache,
    add_parameters,
    decode_base64,
    is_expired,
    to_expiry,
    request_new_grant_with_post
)



class OAuth2ResourceOwnerPasswordCredentials(AuthFlow):
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

        self.token: OAuthToken = None

    async def auth_flow(
        self,
        request: ClientRequest,
        _: Connection
    ) -> AsyncGenerator[None, ClientResponse]:
        if self.token is not None:
            if not is_expired(self.token.expiry, self.early_expiry):
                request.headers[self.header_name] = self.header_value.format(token=self.token.token)
                client_logger.debug(
                    "Using already received authentication, will expire on "
                    f"{datetime.utcfromtimestamp(self.token.expiry)} (UTC)."
                )
                yield
                return
            self.token = None
        
        original_request = request
        cookies = original_request.headers.get(hdrs.COOKIE)
        if cookies:
            new_headers = {hdrs.COOKIE: cookies}
        else:
            new_headers = None

        new_request = ClientRequest(
            method="POST",
            url=self.token_url,
            headers=new_headers,
            data=self.data,
            loop=original_request.loop,
            proxy=original_request.proxy,
            proxy_auth=original_request.proxy_auth,
            session=original_request._session,
            ssl=original_request._ssl,
            proxy_headers=original_request.proxy_headers,
            traces=original_request._traces
        )
        
        response = yield request
        if not 200 <= response.status <= 299:
            yield original_request
            return
        
        content = await response.json()

        token = content.get(self.token_field_name)
        if not token:
            raise GrantNotProvided(self.token_field_name, content)
        
        expires_in = content.get("expires_in")

        if expires_in is None:  # Bearer token
            header, body, other = token.split(".")
            body = json.loads(decode_base64(body))
            expiry = body.get("exp")
            if not expiry:
                raise TokenExpiryNotProvided(expiry)
        else:  # Access Token
            expiry = to_expiry(expires_in)

        client_logger.debug(
            "Using newly received authentication, expiring on "
            f"{datetime.utcfromtimestamp(expiry)} (UTC)."
        )
        self.token = OAuthToken(token=token, expiry=expiry)

        original_request.headers[self.header_name] = self.header_value.format(token=token)
        yield original_request