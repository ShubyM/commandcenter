from commandcenter.core.integrations.exceptions import IntegrationError



class TraxxIntegrationError(IntegrationError):
    """Base exception for Traxx errors."""


class ExpiredSession(TraxxIntegrationError):
    """Raised when the session cookie is expired and we can no longer authenticate
    with the server.
    """

    def __str__(self) -> str:
        return "Signed out. Please refresh session config"