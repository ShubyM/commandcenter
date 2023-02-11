
import os
import pytest


from xprocess import ProcessStarter

from fastapi.testclient import TestClient


@pytest.fixture
def test_client_factory() -> TestClient:
    return TestClient


@pytest.fixture(scope="session")
def ldap_server(xprocess):
    class Starter(ProcessStarter):
        timeout = 5
        pattern = "started on port 3004"

        popen_kwargs = {
            "shell": True,
            "universal_newlines": True,
        }

        config = os.path.abspath("ldap-server-mock-conf.json")
        db = os.path.abspath('users.json')
        
        args = ['npx', 'ldap-server-mock', f'--conf={config}', f'--database={db}']

    # ensure process is running and return its logfile
    xprocess.ensure("ldap-server", Starter)
    yield
    # clean up whole process tree afterwards
    xprocess.getinfo("ldap-server").terminate()