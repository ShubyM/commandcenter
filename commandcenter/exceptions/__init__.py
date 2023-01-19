class CommandCenterException(Exception):
    """Base exception for all command center errors."""


class NotConfigured(CommandCenterException):
    """Raised when a feature is not enabled, usually due to a lack of information
    from the runtime environment.
    
    This exception can be caught in an exception handler to return a special response.
    """