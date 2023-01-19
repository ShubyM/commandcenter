import asyncio
import concurrent.futures
import logging
import os
import pathlib
import subprocess
from typing import List, Optional, Sequence, Union

from commandcenter.core.comm.providers.telalert.models import TelAlertMessage



_LOGGER = logging.getLogger("commandcenter.core.comm.providers")


class TelAlertClient:
    """Async client for sending alerts out through the TelAlert system.

    Args:
        exe_path: Path to the telalert.exe application.
        host: Target host for dial out requests.
    """
    def __init__(
        self,
        path: os.PathLike,
        host: str,
        max_concurrency: int = 4,
        timeout: float = 3
    ) -> None:
        path = pathlib.Path(path)
        if not path.exists():
            raise FileNotFoundError(path.__str__())
        self._path = path
        self._host = host
        self._timeout = timeout

        self._executor: concurrent.futures.ThreadPoolExecutor = concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrency)
        self._lock: asyncio.Semaphore = asyncio.Semaphore(max_concurrency)
        self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    async def send_alert(
        self,
        msg: str,
        groups: Optional[Union[str, Sequence[str]]] = None,
        destinations: Optional[Union[str, Sequence[str]]] = None,
        subject: str = None
    ) -> None:
        """Send a notification through the TelAlert system to any number of
        destinations or groups.
        
        Args
            msg: The message to send.
            groups: The group(s) to send the message to.
            destinations: Individual destinations to send the message to.
            subject: The subject line that appears in email notification destinations.

        Raises
            ValidationError: Invalid message format.
        """
        m = TelAlertMessage(msg=msg, groups=groups, destinations=destinations, subject=subject)
        commands = []
        msg = m.msg
        subject = m.subject or ""
        for group in m.groups:
            commands.append([self._path, "-g", group, "-m", msg, "-host", self._host, "-subject", subject])
        for destination in m.destinations:
            commands.append([self._path, "-i", destination, "-m", msg, "-host", self._host, "-subject", subject])
        tasks = [self._execute_command(command) for command in commands]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result, command in zip(results, commands):
            # A non 0 exit code will not raise an exception so we will not be
            # double logging errors. This way we dont create duplicated events
            # in sentry
            if isinstance(result, BaseException):
                _LOGGER.error(
                    "Notification failed as a result of an exception: %r",
                    result,
                    extra={"Command": command}
                )
        
    async def _execute_command(self, command: List[str]) -> None:
        """Execute the command in a subprocess."""
        async with self._lock:
            try:
                await asyncio.wait_for(
                    self._loop.run_in_executor(
                        self._executor,
                        run_subprocess,
                        command
                    ),
                    timeout=self._timeout
                )
            except asyncio.TimeoutError:
                _LOGGER.warning("Timeout (%0.2f) exceeded on dial out notification", self._timeout)
                raise


def run_subprocess(command: List[str]) -> None:
    """Run a subprocess and route the `stdout` and `stderr` to the logger.
    
    Stdout is debug information, stderr is error, and a non-zero is exit code
    is logged as an error.
    """
    with subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    ) as process:
        for line in process.stdout.readlines():
            _LOGGER.debug(line.decode().rstrip("\r\n"))
        for line in process.stderr.readlines():
            _LOGGER.error(line.decode().rstrip("\r\n"))
    if process.returncode != 0:
        _LOGGER.error("Process exited with exit code %i", process.returncode)