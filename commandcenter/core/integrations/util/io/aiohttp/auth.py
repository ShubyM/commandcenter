from typing import AsyncGenerator

from aiohttp import ClientRequest, ClientResponse
from aiohttp.connector import Connection


class AuthFlow:
    """Base auth flow implementation for `ClientSession`
    
    Auth flows are used as part of custom req/rep handler classes for
    `ClientSession`. Flows are async generator objects that yield requests and
    receive back response objects. Implementations can manipulate headers, cookies,
    etc before sending the next request.
    
    This framework allows for authentication schemes not natively supported in
    aiohttp to be used.
    """
    async def auth_flow(
        self,
        request: ClientRequest,
        connection: Connection
    ) -> AsyncGenerator[None, ClientResponse]:
        # Equivalent to a no auth, send request as is
        yield