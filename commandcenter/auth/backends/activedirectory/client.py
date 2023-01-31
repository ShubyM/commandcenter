import asyncio
import contextlib
import logging
import re
from collections import deque
from collections.abc import Sequence
from typing import AsyncGenerator, Deque

import anyio
from bonsai import LDAPClient, LDAPConnection, LDAPSearchScope
from bonsai.errors import AuthenticationError
from bonsai.pool import ConnectionPool

from commandcenter.auth.backends.activedirectory.discovery import (
    discover_domain,
    discover_domain_controllers
)
from commandcenter.auth.backends.activedirectory.user import ActiveDirectoryUser
from commandcenter.auth.exceptions import UserNotFound
from commandcenter.auth.protocols import AuthenticationClient



_CN_PATTERN = re.compile("(?<=CN=)(.*?)(?=\,)")
_LOGGER = logging.getLogger("commandcenter.auth.activedirectory")


def get_client(
    url: str,
    domain: str,
    mechanism: str,
    tls: bool = False,
    username: str | None = None,
    password: str | None = None,
) -> LDAPClient:
    """Create an `LDAPClient` for authentication and user queries."""
    client = LDAPClient(url, tls)
    client.set_credentials(
        mechanism=mechanism,
        user=username,
        password=password,
        realm=domain
    )
    return client


def get_root_dse(
    url: str,
    domain: str,
    mechanism: str,
    tls: bool = False,
    username: str | None = None,
    password: str | None = None,
) -> str:
    """The root DSE is the base for all LDAP queries."""
    client = get_client(
        url=url,
        domain=domain,
        mechanism=mechanism,
        tls=tls,
        username=username,
        password=password
    )
    root_dse = client.get_rootDSE()
    return root_dse["namingContexts"][0]


def get_pools(
    controllers: Deque[str],
    domain: str,
    mechanism: str,
    maxconn: int,
    tls: bool = False,
    username: str | None = None,
    password: str | None = None,
) -> Deque[ConnectionPool]:
    """Get all connection pools for the number of domain controllers there are."""
    pools = deque()
    for _ in range(len(controllers)):
        url = controllers[0]
        pools.append(
            ConnectionPool(
                get_client(
                    url=url,
                    domain=domain,
                    mechanism=mechanism,
                    tls=tls,
                    username=username,
                    password=password
                ),
                maxconn=maxconn))
        controllers.rotate(1)
    return pools


class ActiveDirectoryClient(AuthenticationClient):
    """Active directory client for handling authentication/authorization within
    a domain.

    You are encouraged to add multiple domain controller hosts (if they exist) for
    resiliency. The client will maintain a set of connection pools to the hosts
    and rotate through them. If one host is unreachable the others can be tried.
    
    Args:
        domain: The domain which the client resides in. This can be auto discovered
            via the `discover_domain` method.
        hosts: A sequence of domain controller hostnames to target. These can be
            auto discovered via the `discover_domain_controllers` method.
        tls: `True` if connection should use TLS.
        maxconn: The maximum LDAP connection pool size.
        mechanism: The authentication mechanism to use on the client. Currently
            only supports SIMPLE and GSSAPI

    Raises:
        LDAPError: Error in LDAPClient when trying to get the rootDSE.
        OSError: `discover_domain` or `discover_domain_controllers` failed.
        NoHostsFound: No hosts found with `discover_domain_controllers`.
        ValueError: Invalid authentication mechanism.

    Note: Bonsai does not support the `ProacterEventLoop` therefore we run
    all I/O in an external threadpool. If proper Windows support comes around,
    this may change.
    """
    def __init__(
        self,
        domain: str | None = None,
        hosts: Sequence[str] | None = None,
        tls: bool = False,
        maxconn: int = 4,
        mechanism: str = "GSSAPI",
        username: str = None,
        password: str = None
    ) -> None:
        mechanism = mechanism.upper()
        if mechanism not in ("GSSAPI", "SIMPLE"):
            raise ValueError(f"Invalid authentication mechanism {mechanism}")

        domain = (domain or discover_domain()).upper()
        hosts = hosts or discover_domain_controllers()
        
        controllers = deque([f"{'ldaps://' if tls else 'ldap://'}{host}" for host in hosts])
        bases = deque(
            [
                get_root_dse(
                    url,
                    domain,
                    mechanism,
                    tls,
                    username,
                    password
                ) for url in controllers
            ]
        )
        pools = get_pools(
            controllers=controllers,
            domain=domain,
            mechanism=mechanism,
            maxconn=maxconn,
            tls=tls,
            username=username,
            password=password
        )

        (self._domain, self._tls, self._mechanism, self._username,
            self._password, self._controllers, self._bases, self._pools) = (
                domain, tls, mechanism, username, password, controllers,
                bases, pools
            )

        self._lock: asyncio.Semaphore = asyncio.Semaphore(maxconn)
        self._limiter: anyio.CapacityLimiter = anyio.CapacityLimiter(maxconn)
        self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    @property
    def controllers(self) -> Deque[str]:
        """Returns the domain controller hosts the client is associated to."""
        return self._controllers

    def rotate(self) -> None:
        """Rotate the domain controller hosts and connection pools."""
        self._controllers.rotate(1)
        self._bases.rotate(1)
        # We dont want to close the pool because another coroutine may be
        # waiting on a response which could trigger a cascade of rotates
        # as that one fails and rotates and so on...
        self._pools.rotate(1)
        _LOGGER.debug("Client rotated")

    @contextlib.asynccontextmanager
    async def _get_connection(self) -> AsyncGenerator[LDAPConnection, None]:
        """Acquire an `LDAPConnection` from the pool."""
        pool = self._pools[0]
        url = self._controllers[0]
        async with self._lock:
            if pool.closed:
                await anyio.to_thread.run_sync(pool.open, limiter=self._limiter)
            try:
                conn = await anyio.to_thread.run_sync(pool.get, limiter=self._limiter)
                _LOGGER.debug("Connected to %s", url)
                yield conn
            finally:
                _LOGGER.debug("Connection released")
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
        url = self._controllers[0]
        client = get_client(
            url=url,
            domain=self._domain,
            mechanism=self._mechanism,
            tls=self._tls,
            username=username,
            password=password
        )
        try:
            with await anyio.to_thread.run_sync(client.connect, limiter=self._limiter):
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
            base = self._bases[0]
            results = await anyio.to_thread.run_sync(
                conn.search,
                base,
                LDAPSearchScope.SUB,
                f"(&(objectCategory=user)(sAMAccountName={username}))",
                limiter=self._limiter
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