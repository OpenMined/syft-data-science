from pathlib import Path

import pytest
from syft_core import Client, SyftClientConfig
from syft_rds.client.rds_client import RDSClient, init_session
from syft_rds.server.app import create_app

TEST_SERVER_USER = "alice@openmined.org"


@pytest.fixture
def syftbox_server_client(tmp_path):
    return Client(
        SyftClientConfig(
            email=TEST_SERVER_USER,
            client_url="http://localhost:5000",
            path=tmp_path / "syftbox_client_config.json",
        ),
    )


@pytest.fixture
def rds_client(syftbox_server_client: Client) -> RDSClient:
    rds_app = create_app(syftbox_server_client)
    return init_session("alice@openmined.org", mock_server=rds_app)


def test_create_job(rds_client: RDSClient):
    job = rds_client.jobs.submit(name="My Job", user_code_path="~/my_user_code.py")
    assert job.name == "My Job"
    assert job.user_code.path == Path("~/my_user_code.py")
