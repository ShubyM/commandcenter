import pytest
from functools import partial

from commandcenter.auth.models import BaseUser
from commandcenter.auth.models import TokenHandler
from commandcenter.auth.scopes import requires

from commandcenter.auth.backends.activedirectory import ActiveDirectoryClient
from commandcenter.auth.backends.activedirectory import ActiveDirectoryBackend

from commandcenter.exceptions import NotConfigured
from commandcenter.config.auth import (
    CC_AUTH_ALGORITHM,
    CC_AUTH_TOKEN_EXPIRE
)

from fastapi import FastAPI, Request, Depends
from fastapi.testclient import TestClient
from fastapi.routing import APIRouter

from starlette.middleware import Middleware
from starlette.middleware.authentication import AuthenticationMiddleware


### Fixtures ###
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
def token_handler() -> TokenHandler:
    return TokenHandler(
        key='secret',
        expire=CC_AUTH_TOKEN_EXPIRE,
        algorithm=CC_AUTH_ALGORITHM
    )


@pytest.fixture
@pytest.mark.asyncio
async def ad_backend(ad_client, token_handler) -> ActiveDirectoryBackend:
    return partial(
        ActiveDirectoryBackend,
        client=ad_client(),
        handler=token_handler
    )


@pytest.fixture
def ad_app(ad_backend) -> FastAPI:
    app = FastAPI(
        title="Test Authentication",
        middleware=[
            Middleware(AuthenticationMiddleware, backend=ad_backend())
        ]
    )

    router = APIRouter()

    @router.get("/whoami", response_model=BaseUser)
    def whoami(request: Request):
        return request.user.dict()

    @router.get("/is_authenticated", response_model=bool)
    def is_authenticated(request: Request):
        return request.user.is_authenticated

    @router.get("/requires_admin_scopes", response_model=BaseUser)
    def requires_admin(request: Request, user: BaseUser = Depends(requires(["ADMIN"]))):
        return user.dict()

    @router.get("/requires_scopes")
    def requires_scopes(request: Request, user: BaseUser = Depends(requires(raise_on_no_scopes=True))):
        return user.dict()

    app.include_router(router)

    return app


@pytest.fixture
def ad_test_client(test_client_factory, ad_app) -> TestClient:
    with test_client_factory(ad_app) as client:
        yield client


### Tests ###
@pytest.mark.asyncio
async def test_authentication_client_get_user(ad_client: partial[ActiveDirectoryClient]):
    admin: BaseUser = await ad_client().get_user("admin")
    assert admin == TestUser

@pytest.mark.asyncio
async def test_authentication_client_authenticate(ad_client: partial[ActiveDirectoryClient]):
    # Assming simple bind with anonymous user, this works
    assert await ad_client().authenticate("", "")

@pytest.mark.asyncio
async def test_authentication_backend_valid_headers(ad_test_client: TestClient, token_handler: TokenHandler):
    token = token_handler.issue({"sub": "admin"})
    ad_test_client.headers = {"Authorization": f"Bearer {token}"}

    response = ad_test_client.get("/whoami")

    assert response.status_code == 200

    user_dict = TestUser.dict()
    user_dict["scopes"] = list(user_dict["scopes"])

    assert response.json() == user_dict


@pytest.mark.asyncio
async def test_authentication_backend_invalid_headers(ad_test_client: TestClient):
    response = ad_test_client.get("/is_authenticated")
    assert response.status_code == 200
    assert response.content == b"false"


@pytest.mark.asyncio
async def test_authentication_scopes_valid_scopes(ad_test_client, token_handler):
    token = token_handler.issue({"sub": "admin"})
    ad_test_client.headers = {"Authorization": f"Bearer {token}"}
    assert ad_test_client.get("requires_admin_scopes").status_code == 200


@pytest.mark.asyncio
async def test_authentication_scopes_invalid_scopes(ad_test_client: TestClient, token_handler: TokenHandler):
    token = token_handler.issue({"sub": "nonadmin"})
    ad_test_client.headers = {"Authorization": f"Bearer {token}"}
    assert ad_test_client.get("/requires_admin_scopes").status_code == 403

@pytest.mark.asyncio
async def test_authentication_scopes_raise_exception_no_scope(ad_test_client: TestClient, token_handler: TokenHandler):
    with pytest.raises(NotConfigured):
        token = token_handler.issue({"sub": "nonadmin"})
        ad_test_client.headers = {"Authorization": f"Bearer {token}"}
        ad_test_client.get("/requires_scopes")
