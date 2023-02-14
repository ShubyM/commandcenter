import pytest

from functools import partial

from commandcenter.auth.models import BaseUser
from commandcenter.auth.models import TokenHandler

from commandcenter.auth.backends.activedirectory import ActiveDirectoryBackend
from commandcenter.auth.backends.activedirectory import ActiveDirectoryClient

from commandcenter.api.config.auth import (
    CC_AUTH_ALGORITHM,
    CC_AUTH_SECRET_KEY,
    CC_AUTH_TOKEN_EXPIRE
)

from fastapi import FastAPI 

from starlette.responses import JSONResponse
from starlette.middleware import Middleware
from starlette.authentication import requires
from starlette.middleware.authentication import AuthenticationMiddleware


@pytest.fixture
def auth_client() -> partial[ActiveDirectoryClient]:
    return partial(
        ActiveDirectoryClient, 
        domain="",
        hosts=["127.0.0.1:3004"],
        tls=False,
        mechanism="Simple",
    )

@pytest.mark.asyncio
async def test_authentication_client_get_user(auth_client: partial[ActiveDirectoryClient], ldap_server):
    admin: BaseUser = await auth_client().get_user("admin")

    # TODO: refactor to constants later
    assert admin == BaseUser(
        username="admin",
        first_name="Shuby",
        last_name="Mishra",
        email="shubymishra20@gmail.com",
        upi="12345678",
        company="abbvie",
        country="USA",
        scopes=set(),
    )

@pytest.mark.asyncio
async def test_authentication_client_authenticate(auth_client: partial[ActiveDirectoryClient] , ldap_server):
    # Assming simple bind with anonymous user, this works
    assert await auth_client().authenticate("", "")

# add test with our enviorment variabels used

@pytest.mark.asyncio
async def test_authentication_backend_ad(auth_client, test_client_factory, ldap_server):
    # takes in handler and client

    client: ActiveDirectoryClient = auth_client()

    handler: TokenHandler = TokenHandler(
        key=str(CC_AUTH_SECRET_KEY),
        expire=CC_AUTH_TOKEN_EXPIRE,
        algorithm=CC_AUTH_ALGORITHM
    )

    backend: ActiveDirectoryBackend = ActiveDirectoryBackend(
        handler = handler,
        client = client
    )

    middleware = [
        Middleware(AuthenticationMiddleware, backend = backend)
    ]

    app = FastAPI(middleware=middleware)


    @requires("authenticated")
    @app.get("/")
    def endpoint(request):
        return request



    with test_client_factory(app) as client:
        print(client.get("/").content)


    







