import pytest

from fastapi.testclient import TestClient


@pytest.fixture
def test_client_factory() -> TestClient:
    return TestClient