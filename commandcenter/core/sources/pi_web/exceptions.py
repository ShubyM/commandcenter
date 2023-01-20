from typing import List

from commandcenter.core.integrations.exceptions import IntegrationError



class PIWebIntegrationError(IntegrationError):
    """Base exception for pi_web integration errors."""


class PIWebContentError(PIWebIntegrationError):
    """Raised when a required property is not present in the body of a successful
    response.
    """


class PIWebResponseError(PIWebIntegrationError):
    """Raised when the `Errors` property is present in the body of successful
    HTTP response.
    """
    def __init__(self, errors: List[str]) -> None:
        self.errors = errors

    def __str__(self) -> str:
        return "{} errors returned in response body.".format(len(self.errors))