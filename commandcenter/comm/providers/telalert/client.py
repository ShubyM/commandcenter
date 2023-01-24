import asyncio
import concurrent.futures
import logging
import os
import pathlib
from typing import List

import anyio

from commandcenter.comm.providers.telalert.models import TelAlertMessage
from commandcenter.util.process import run_subprocess



_LOGGER = logging.getLogger("commandcenter.comm.providers.telalert")


class TelAlertClient:
    """Client for sending alerts through the TelAlert system.

    Args:
        path: Path to the `telalert.exe` application.
        host: Target host for dial out requests.
        max_workers: The number of sub processes that can be run concurrently.
        timeout: The max time for a call to `telalert.exe` to complete.

    Raises:
        FileNotFoundError: The path to `telalert.exe` was not found.

    Examples:
    >>> client = TelAlertClient(path, "myhost")
    ... # Send a notification to a group
    ... await send_alert("Something happened", groups=["mygroup"])
    ... # You can send alerts to multiple groups in one call
    ... await send_alert("Its bad guys", groups=["mygroup", "thatgroup"])
    ... # You can also mix and match groups and destinations
    ... await send_alert("Dont tell him", groups=["mygroup", "thatgroup"], destinations=["CEO"])
    """
    def __init__(
        self,
        path: os.PathLike,
        host: str,
        max_workers: int = 4,
        timeout: float = 3
    ) -> None:
        path = pathlib.Path(path)
        if not path.exists():
            raise FileNotFoundError(path.__str__())
        self._path = path
        self._host = host
        self._timeout = timeout

        self._executor: concurrent.futures.ThreadPoolExecutor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self._lock: asyncio.Semaphore = asyncio.Semaphore(max_workers)
        self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    async def send_alert(self, msg: TelAlertMessage) -> None:
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
        commands = []
        msg = msg.msg
        subject = msg.subject or ""
        for group in msg.groups:
            commands.append([self._path, "-g", group, "-m", msg, "-host", self._host, "-subject", subject])
        for destination in msg.destinations:
            commands.append([self._path, "-i", destination, "-m", msg, "-host", self._host, "-subject", subject])
        tasks = [self._execute_command(command) for command in commands]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for maybe_exc, command in zip(results, commands):
            if isinstance(maybe_exc, BaseException):
                _LOGGER.error("Notification failed", exc_info=maybe_exc, extra={"command": command})
        
    async def _execute_command(self, command: List[str]) -> None:
        """Execute the command in a subprocess."""
        async with self._lock:
            with anyio.fail_after(self._timeout):
                await anyio.to_thread.run_sync(run_subprocess, command)