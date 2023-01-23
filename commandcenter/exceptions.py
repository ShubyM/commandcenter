class CommandCenterException(Exception):
    """Base exception for all commandcenter errors."""


class NotConfigured(CommandCenterException):
    """Raised when a feature is not enabled, usually due to a missing requirment
    in the config.
    """