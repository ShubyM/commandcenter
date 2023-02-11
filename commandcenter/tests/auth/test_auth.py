import pytest
import subprocess


from commandcenter.auth.models import BaseUser
from commandcenter.auth import AuthenticationClient
from commandcenter.api.config.scopes import ADMIN_USER


from commandcenter.auth.backends.activedirectory import ActiveDirectoryBackend
from commandcenter.auth.backends.activedirectory import ActiveDirectoryClient

@pytest.mark.asyncio
async def test_authentication_client_get_user(ldap_server):
    # might be able to make backend a fixture

    client: AuthenticationClient = ActiveDirectoryClient(
        domain="",
        hosts=["127.0.0.1:3004"],
        tls=False,
        mechanism="Simple",
    )


    await client.get_user("wow")

    # # consider adding some test user into the LDAP client
    # assert admin_information.username == "ADMIN"


# @pytest.mark.asyncio
# async def test_authentication_client_authenticate():
    # client: AuthenticationClient = ActiveDirectoryClient(
    #     hosts="127.0.0.1:3004",
    #     tls=True,
    # )
    # client: AuthenticationClient = setup_auth_backend().client
    # # call the the authenticate method with some test username and password
    # res = await client.authenticate("NOT A USERNAME", "NOT A PASSWORD")
    # assert res == False
    



