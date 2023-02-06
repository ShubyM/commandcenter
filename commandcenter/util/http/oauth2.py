import base64
from datetime import datetime, timedelta, timezone
from typing import Any, Dict
from urllib.parse import parse_qs, urlsplit, urlunsplit, urlencode

from pydantic import BaseModel

from commandcenter.exceptions import CommandCenterException



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
    """`True` if token is expired."""
    return (
        datetime.utcfromtimestamp(expiry - early_expiry)
        < datetime.utcnow()
    )


def to_expiry(expires_in: int | str) -> float:
    """Convert response expiry into timestamp."""
    expiry = datetime.utcnow().replace(
        tzinfo=timezone.utc
    ) + timedelta(seconds=int(expires_in))
    return expiry.timestamp()


class OAuthToken(BaseModel):
    token: str
    expiry: float


class OAuthError(CommandCenterException):
    """Base exception for OAuth errors."""


class InvalidToken(OAuthError):
    """Token is invalid."""

    def __init__(self, token_name: str):
        OAuthError.__init__(self, f"{token_name} is invalid.")


class GrantNotProvided(OAuthError):
    """Grant was not provided."""

    def __init__(self, grant_name: str, dictionary_without_grant: dict):
        OAuthError.__init__(
            self, f"{grant_name} not provided within {dictionary_without_grant}."
        )


class TokenExpiryNotProvided(OAuthError):
    """Token expiry was not provided."""

    def __init__(self, token_body: dict):
        OAuthError.__init__(self, f"Expiry (exp) is not provided in {token_body}.")


class InvalidGrantRequest(OAuthError):
    """If the request failed client authentication or is invalid, the authorization
    server returns an error response as described in https://tools.ietf.org/html/rfc6749#section-5.2
    """

    # https://tools.ietf.org/html/rfc6749#section-5.2
    request_errors = {
        "invalid_request": "The request is missing a required parameter, includes "
            "an unsupported parameter value (other than grant type), repeats a "
            "parameter, includes multiple credentials, utilizes more than one "
            "mechanism for authenticating the client, or is otherwise malformed.",
        "invalid_client": "Client authentication failed (e.g., unknown client, "
            "no client authentication included, or unsupported authentication "
            "method).  The authorization server MAY return an HTTP 401 "
            "(Unauthorized) status code to indicate which HTTP authentication "
            "schemes are supported.  If the client attempted to authenticate via "
            "the 'Authorization' request header field, the authorization server "
            "MUST respond with an HTTP 401 (Unauthorized) status code and include "
            "the 'WWW-Authenticate' response header field matching the "
            "authentication scheme used by the client.",
        "invalid_grant": "The provided authorization grant (e.g., authorization "
            "code, resource owner credentials) or refresh token is invalid, "
            "expired, revoked, does not match the redirection URI used in the "
            "authorization request, or was issued to another client.",
        "unauthorized_client": "The authenticated client is not authorized to "
            "use this authorization grant type.",
        "unsupported_grant_type": "The authorization grant type is not supported "
            "by the authorization server.",
        "invalid_scope": "The requested scope is invalid, unknown, malformed, or "
            "exceeds the scope granted by the resource owner.",
    }

    def __init__(self, content: Dict[Any, Any]):
        OAuthError.__init__(self, InvalidGrantRequest.to_message(content))

    @staticmethod
    def to_message(content: Dict[Any, Any], errors: Dict[str, str]) -> str:
        """Handle response as described in:
            * https://tools.ietf.org/html/rfc6749#section-5.2
            * https://tools.ietf.org/html/rfc6749#section-4.1.2.1
            * https://tools.ietf.org/html/rfc6749#section-4.2.2.1
        """
        def _pop(key: str) -> str:
            value = content.pop(key, None)
            if value and isinstance(value, list):
                value = value[0]
            return value

        if "error" in content:
            error = _pop("error")
            error_description = _pop("error_description") or errors.get(error)
            message = f"{error}: {error_description}"
            if "error_uri" in content:
                message += f"\nMore information can be found on {_pop('error_uri')}"
            if content:
                message += f"\nAdditional information: {content}"
        else:
            message = f"{content}"
        return message