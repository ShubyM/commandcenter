import pytest


from commandcenter.auth.models import BaseUser
from commandcenter.auth import AuthenticationClient
from commandcenter.api.config.scopes import ADMIN_USER
from commandcenter.api.setup.auth import setup_auth_backend




# @pytest.fixture
# async def auth_client():
#     return setup_auth_backend().client


@pytest.mark.asyncio
async def test_authentication_client_get_user():
    # might be able to make backend a fixture

    client: AuthenticationClient = setup_auth_backend().client
    admin_information: BaseUser = await client.get_user(ADMIN_USER.username)
    # consider adding some test user into the LDAP client
    assert admin_information.username == "ADMIN"


@pytest.mark.asyncio
async def test_authentication_client_authenticate():
    client: AuthenticationClient = setup_auth_backend().client
    # call the the authenticate method with some test username and password
    res = await client.authenticate("NOT A USERNAME", "NOT A PASSWORD")
    assert res == False
    



