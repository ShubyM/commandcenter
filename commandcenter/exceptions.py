class CommandCenterException(Exception):
    """Base exception for all commandcenter errors."""


class NotConfigured(CommandCenterException):
    """Raised when a feature is not enabled, usually due to a missing requirment
    in the config.
    """


class SubProcessError(CommandCenterException):
    """Base exception for subprocess errors."""


class NonZeroExitCode(SubProcessError):
    """Raised when a process exits with a non-zero exit code."""
    def __init__(self, code: int) -> None:
        self.code = code

    def __str__(self) -> str:
        return "Process exited with non-zero exit code ({}).".format(self.code)