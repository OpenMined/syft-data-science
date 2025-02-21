import pytest
from pathlib import Path
import json

from syft_core import Client as SyftBoxClient
from syft_core.config import SyftClientConfig
from syft_rds.client.rds_client import RDSClient


SYFTBOX_CLIENT_EMAIL = "test1@openmined.org"
HOST_EMAIL = "test2@openmined.org"


@pytest.fixture
def syftbox_client(tmp_path: Path) -> SyftBoxClient:
    return SyftBoxClient(
        SyftClientConfig(
            email=SYFTBOX_CLIENT_EMAIL,
            client_url="http://localhost:5000",
            path=tmp_path / "syftbox_client_config.json",
            data_dir=tmp_path / "SyftBoxTest",
        )
    )


@pytest.fixture
def rds_client(syftbox_client: SyftBoxClient) -> RDSClient:
    return RDSClient(host=HOST_EMAIL, syftbox_client=syftbox_client)


def test_rdsclient_rpc_send(rds_client: RDSClient) -> None:
    future = rds_client.jobs.submit(
        name="My test job",
        description="text text",
        runtime="python 3.12",
        tags=["a", "b"],
    )
    create_future_dir = Path(future.path)
    request_file_path = create_future_dir / (str(future.id) + ".request")
    assert create_future_dir.is_dir()
    assert request_file_path.is_file()

    with open(request_file_path, "r") as file:
        request_file_content: dict = json.load(file)
        assert request_file_content["id"] == str(future.id)
        assert request_file_content["sender"] == rds_client.syftbox_client.email
        assert rds_client.host in request_file_content["url"]
