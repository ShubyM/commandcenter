import asyncio
import concurrent.futures
import contextlib
import logging
import re
from collections import deque
from typing import AsyncGenerator, Deque, List, Optional, Sequence, Tuple

from bonsai import LDAPClient, LDAPConnection, LDAPSearchScope
from bonsai.errors import AuthenticationError
from bonsai.pool import ConnectionPool

from commandcenter.core.auth.backends.ad.discovery import (
    discover_domain,
    discover_domain_controllers
)
from commandcenter.core.auth.backends.ad.models import ActiveDirectoryUser
from commandcenter.core.auth.exceptions import UserNotFound
from commandcenter.core.objcache import memo



_CN_PATTERN = re.compile("(?<=CN=)(.*?)(?=\,)")
_LOGGER = logging.getLogger("commandcenter.core.auth.ad")


@memo
def get_root_dse(url: str, domain: str) -> str:
    """The root DSE is the base for all LDAP queries."""
    client = LDAPClient(url)
    client.set_credentials("GSSAPI", user=None, password=None, realm=domain)
    root_dse = client.get_rootDSE()
    return root_dse["namingContexts"][0]


class ActiveDirectoryClient:
    """Active directory client for handling authentication/authorization within
    a domain.

    You are encouraged to add multiple domain controller hosts (if they exist) for
    resiliency. The client will maintain a set of connection pools to the hosts
    and rotate through them. If one host is unreachable the others can be tried.
    
    Args:
        domain: The domain which the client resides in. This can be auto discovered
            via the `discover_domain` method.
        dc_hosts: A sequence of domain controller hostnames to target. These can be
            auto discovered via the `discover_domain_controllers` method.
        tls: `True` if connection should use TLS.
        maxconn: The maximum LDAP connection pool size.
    
    Raises:
        LDAPError: Error in LDAPClient when trying to get the rootDSE.

    Note: Bonsai does not support the `ProacterEventLoop` therefore we run
    all I/O in an external threadpool. If proper Windows support comes around,
    this may change.
    """
    def __init__(
        self,
        domain: Optional[str] = None,
        dc_hosts: Optional[Sequence[str]] = None,
        tls: bool = False,
        maxconn: int = 4
    ) -> None:
        domain = domain or discover_domain()
        dc_hosts = dc_hosts or discover_domain_controllers()

        self._domain = domain.upper()
        self._tls = tls
        
        urls = [f"{'ldaps://' if tls else 'ldap://'}{host}" for host in dc_hosts]
        bases = [get_root_dse(url, domain) for url in urls]
        self._dcs: Deque[Tuple[str, str]] = deque([(url, base) for url, base in zip(urls, bases)])
        self._pools: Deque[ConnectionPool] = self._get_pools(maxconn)

        self._lock: asyncio.Semaphore = asyncio.Semaphore(maxconn)
        self._executor: concurrent.futures.ThreadPoolExecutor = concurrent.futures.ThreadPoolExecutor(max_workers=maxconn)
        self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    def _get_client(self,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ) -> LDAPClient:
        """Create an `LDAPClient` for authentication and user queries."""
        client = LDAPClient(self._dcs[0][0], self._tls)
        client.set_credentials("GSSAPI", user=username, password=password, realm=self._domain)
        return client

    def _get_pools(self, maxconn: int) -> Deque[ConnectionPool]:
        """Get all connection pools for the number of domain controllers there are."""
        pools = deque()
        for _ in range(len(self._dcs)):
            pools.append(ConnectionPool(self._get_client(), maxconn=maxconn))
            self._dcs.rotate(1)
        return pools

    def rotate(self) -> None:
        """Rotate the domain controller hosts and connection pools."""
        self._dcs.rotate(1)
        # We dont want to close the pool because another coroutine may be
        # waiting on a response which could trigger a cascade of rotates
        # as that one fails and rotates and so on...
        self._pools.rotate(1)
        _LOGGER.debug("Connection pools rotated")

    @contextlib.asynccontextmanager
    async def _get_connection(self) -> AsyncGenerator[LDAPConnection, None]:
        """Acquire an `LDAPConnection` from the pool."""
        pool = self._pools[0]
        dc = self._dcs[0]
        async with self._lock:
            if pool.closed:
                # open will create a connection so we execute in a threadpool
                await self._loop.run_in_executor(self._executor, pool.open)
            try:
                conn = await self._loop.run_in_executor(self._executor, pool.get)
                _LOGGER.debug("Connection acquired to %s", dc)
                yield conn
            finally:
                pool.put(conn)

    async def authenticate(self, username: str, password: str) -> bool:
        """Username and password authentication.
        
        This method only verifies the username and password are valid based on
        the ability to create a connection to the server.

        Args:
            username: Username.
            password: Password.

        Returns:
            bool: `True` if user is authenticated, `False` otherwise.

        Raises:
            LDAPError: Error in LDAPClient.
        """
        client = self._get_client(username, password)
        try:
            with await self._loop.run_in_executor(self._executor, client.connect):
                return True
        except AuthenticationError:
            return False

    async def get_user(self, username: str) -> ActiveDirectoryUser:
        """Retrieve a user object from the underlying database.
        
        Args:
            username: Username.

        Returns:
            user: Instance of `ActiveDirectoryUser`.

        Raise:
            UserNotFound: The query returned no results.
            LDAPError: Error in `LDAPClient`.
        """
        async with self._get_connection() as conn:
            results = await self._loop.run_in_executor(
                self._executor,
                conn.search,
                self._dcs[0][1],
                LDAPSearchScope.SUB,
                f"(&(objectCategory=user)(sAMAccountName={username}))"
            )
            
            if len(results) < 1:
                raise UserNotFound()
            # sAMAccount name must be unique
            assert len(results) == 1
            
            result = results[0]
            scopes = set()
            for group in result["memberOf"]:
                match = _CN_PATTERN.search(group)
                if match is not None:
                    scopes.add(match.group(1))
            
            return ActiveDirectoryUser(scopes=scopes, **result)