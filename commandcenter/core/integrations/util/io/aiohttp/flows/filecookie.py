import os
import pathlib
from typing import AsyncGenerator

import toml
from aiohttp import ClientRequest, ClientResponse
from aiohttp.connector import Connection
from commandcenter.core.integrations.util.io.aiohttp import AuthFlow



class FileCookieAuthFlow(AuthFlow):
    """Auth flow for reading cookie authentication headers from TOML.
    
    Args:
        path: The path to the .toml file
    """
    def __init__(self, path: os.PathLike) -> None:
        path = pathlib.Path(path)
        if not path.exists():
            raise FileNotFoundError(path.__str__())
        self._path = path

    async def auth_flow(
        self,
        request: ClientRequest,
        _: Connection
    ) -> AsyncGenerator[None, ClientResponse]:
        cookies = toml.loads(self._path.read_text())
        request.update_cookies(cookies)
        yield