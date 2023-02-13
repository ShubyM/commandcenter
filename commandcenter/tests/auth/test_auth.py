import pytest


from functools import partial

from commandcenter.auth.models import BaseUser

from commandcenter.auth.backends.activedirectory import ActiveDirectoryBackend
from commandcenter.auth.backends.activedirectory import ActiveDirectoryClient



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

async def test_authentication_backend_authenticate(test_client_factory, ldap_server):
    backend: ActiveDirectoryBackend = ActiveDirectoryBackend(

    )






