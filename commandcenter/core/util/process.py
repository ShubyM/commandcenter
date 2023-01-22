import logging
import subprocess
from typing import List

from commandcenter.exceptions import CommandCenterException



_LOGGER = logging.getLogger("commandcenter.core.util")


class SubProcessError(CommandCenterException):
    """Base exception for subprocess errors."""


class NonZeroExitCode(SubProcessError):
    """Raised when a process exits with a non-zero exit code."""
    def __init__(self, code: int) -> None:
        self.code = code

    def __str__(self) -> str:
        return "Process exited with non-zero exit code ({}).".format(self.code)


def run_subprocess(command: List[str], raise_non_zero: bool = True) -> None:
    """Run a subprocess and route the `stdout` and `stderr` to the logger.
    
    Stdout is debug information and stderr is warning.

    Raises:
        NonZeroExitCode: Process exited with non-zero exit code.
    """
    with subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    ) as process:
        for line in process.stdout.readlines():
            _LOGGER.debug(line.decode().rstrip("\r\n"))
        for line in process.stderr.readlines():
            _LOGGER.warning(line.decode().rstrip("\r\n"))
    if process.returncode != 0:
        _LOGGER.warning("Process exited with non-zero exit code (%i)", process.returncode)
        if raise_non_zero:
            raise NonZeroExitCode(process.returncode)