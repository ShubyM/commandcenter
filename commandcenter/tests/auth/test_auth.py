import pytest

from functools import partial

from commandcenter.auth.models import BaseUser
from commandcenter.auth.models import TokenHandler

from commandcenter.auth.backends.activedirectory import ActiveDirectoryClient
from commandcenter.auth.backends.activedirectory import ActiveDirectoryBackend

from commandcenter.api.config.auth import (
    CC_AUTH_ALGORITHM,
    CC_AUTH_SECRET_KEY,
    CC_AUTH_TOKEN_EXPIRE
)

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from starlette.middleware import Middleware
from starlette.middleware.authentication import AuthenticationMiddleware


TEST_USER = BaseUser(
    username="admin",
    first_name="Shuby",
    last_name="Mishra",
    email="shubymishra20@gmail.com",
    upi="12345678",
    company="abbvie",
    country="USA",
    scopes=set(),
)

@pytest.fixture
def ad_client() -> partial[ActiveDirectoryClient]:
    return partial(
        ActiveDirectoryClient, 
        domain="",
        hosts=["127.0.0.1:3004"],
        tls=False,
        mechanism="Simple",
    )

@pytest.fixture
def env_token_handler() -> TokenHandler:
    return TokenHandler(
        key='secret',
        expire=CC_AUTH_TOKEN_EXPIRE,
        algorithm=CC_AUTH_ALGORITHM
    )

@pytest.fixture
@pytest.mark.asyncio
async def ad_backend(ad_client, env_token_handler) -> ActiveDirectoryBackend:
    return partial(
        ActiveDirectoryBackend,
        client=ad_client(),
        handler=env_token_handler
    )

@pytest.mark.asyncio
async def test_authentication_client_get_user(ad_client: partial[ActiveDirectoryClient], ldap_server):
    admin: BaseUser = await ad_client().get_user("admin")
    assert admin == TEST_USER

@pytest.mark.asyncio
async def test_authentication_client_authenticate(ad_client: partial[ActiveDirectoryClient] , ldap_server):
    # Assming simple bind with anonymous user, this works
    assert await ad_client().authenticate("", "")

@pytest.mark.asyncio
async def test_authentication_backend_ad(ad_backend, test_client_factory, ldap_server):
    # takes in handler and client

    backend: ActiveDirectoryBackend = ad_backend()

    middleware = [
        Middleware(AuthenticationMiddleware, backend = backend)
    ]

    app = FastAPI(middleware=middleware)

    @app.get("/", response_model=BaseUser)
    def endpoint(request: Request):
        return request.user.dict()

    with test_client_factory(app) as client: 
        client: TestClient

        # send a request as the admin user
        token = backend.handler.issue({"sub": "admin"})

        client.headers = {
            "Authorization": f"Bearer {token}",
        }

        response = client.get("/")

        assert response.status_code == 200

