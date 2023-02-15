import pytest
from functools import partial

from commandcenter.auth.models import BaseUser
from commandcenter.auth.models import TokenHandler
from commandcenter.auth.scopes import requires

from commandcenter.auth.backends.activedirectory import ActiveDirectoryClient
from commandcenter.auth.backends.activedirectory import ActiveDirectoryBackend

from commandcenter.exceptions import NotConfigured
from commandcenter.api.dependencies import get_auth_client
from commandcenter.api.config.auth import (
    CC_AUTH_ALGORITHM,
    CC_AUTH_SECRET_KEY,
    CC_AUTH_TOKEN_EXPIRE
)

from fastapi import FastAPI, Request, Depends
from fastapi.testclient import TestClient

from starlette.middleware import Middleware
from starlette.middleware.authentication import AuthenticationMiddleware

# read this in from file
TestUser = BaseUser(
    username="admin",
    first_name="Shuby",
    last_name="Mishra",
    email="shubymishra20@gmail.com",
    upi="12345678",
    company="abbvie",
    country="USA",
    scopes=set(["ADMIN"]),
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

@pytest.fixture
def ad_app(ad_backend) -> FastAPI:
    return FastAPI(middleware=[Middleware(
        AuthenticationMiddleware,
        backend = ad_backend()
    )])

@pytest.mark.asyncio
async def test_authentication_client_get_user(ad_client: partial[ActiveDirectoryClient], ldap_server):
    admin: BaseUser = await ad_client().get_user("admin")
    assert admin == TestUser

@pytest.mark.asyncio
async def test_authentication_client_authenticate(ad_client: partial[ActiveDirectoryClient] , ldap_server):
    # Assming simple bind with anonymous user, this works
    assert await ad_client().authenticate("", "")

@pytest.mark.asyncio
async def test_authentication_backend_valid_headers(ad_app, env_token_handler, test_client_factory, ldap_server):
    @ad_app.get("/", response_model=BaseUser)
    def endpoint(request: Request):
        return request.user.dict()

    with test_client_factory(ad_app) as client: 
        client: TestClient

        token = env_token_handler.issue({"sub": "admin"})

        client.headers = {
            "Authorization": f"Bearer {token}",
        }

        response = client.get("/")

        assert response.status_code == 200
        
        user_dict = TestUser.dict()
        user_dict["scopes"] = list(user_dict["scopes"])

        assert response.json() == user_dict

@pytest.mark.asyncio
async def test_authentication_backend_invalid_headers(ad_app, test_client_factory, ldap_server):
    @ad_app.get("/")
    def endpoint(request: Request):
        return {
            "is_authenticated": request.user.is_authenticated,
            "username": request.user.display_name
        }

    with test_client_factory(ad_app) as client: 
        client: TestClient
        response = client.get("/")

        assert response.status_code == 200

        user = response.json()
        assert user["is_authenticated"] == False


@pytest.mark.asyncio
async def test_authentication_scopes_valid_scopes(ad_app: FastAPI, test_client_factory: TestClient, env_token_handler: TokenHandler, ldap_server):
    @ad_app.get("/", dependencies=[Depends(requires(["ADMIN"]))])
    def endpoint(request: Request):
        return {"valid": "true"}

    with test_client_factory(ad_app) as client:
        client: TestClient
        token = env_token_handler.issue({"sub": "admin"})
        client.headers = {"Authorization": f"Bearer {token}"}
        assert client.get("/").status_code == 200

@pytest.mark.asyncio
async def test_authentication_scopes_invalid_scopes(ad_app: FastAPI, test_client_factory: TestClient, env_token_handler: TokenHandler, ldap_server):
    @ad_app.get("/", dependencies=[Depends(requires(["ADMIN"]))])
    def endpoint(request: Request):
        return {"valid": "true"}

    with test_client_factory(ad_app) as client:
        client: TestClient
        token = env_token_handler.issue({"sub": "nonadmin"})
        client.headers = {"Authorization": f"Bearer {token}"}
        assert client.get("/").status_code == 403

@pytest.mark.asyncio
async def test_authentication_scopes_raise_exception_no_scope(ad_app: FastAPI, test_client_factory: TestClient, env_token_handler: TokenHandler, ldap_server):
    @ad_app.get("/", dependencies=[Depends(requires(raise_on_no_scopes=True))])
    def endpoint(request: Request):
        return {"valid": "true"}

    with pytest.raises(NotConfigured):
        with test_client_factory(ad_app) as client:
            client: TestClient
            token = env_token_handler.issue({"sub": "nonadmin"})
            client.headers = {"Authorization": f"Bearer {token}"}
            client.get("/")
        