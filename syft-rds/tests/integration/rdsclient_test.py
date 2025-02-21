from loguru import logger
import pytest

from syft_core import Client, SyftClientConfig
from syft_rds.client.rds_client import RDSClient
from syft_rds.services.dataset.dataset_models import CreateDataset


TEST_EMAIL = "khoa@openmined.org"


@pytest.fixture
def host_syftbox_client(tmp_path):
    logger.debug(f"Creating a SyftBox client for testing at {tmp_path}")
    return Client(
        SyftClientConfig(
            email=TEST_EMAIL,
            client_url="http://localhost:5000",
            path=tmp_path / "syftbox_client_config.json",
        )
    )


def test_rdsclient_rpc_send(host_syftbox_client):
    logger.debug(f"host_syftbox_client = {host_syftbox_client}")
    dataset = CreateDataset(
        name="Census Dataset",
        description="Census Dataset for the year 1994",
        tags=["Census", "1994"],
        private_data_path="./data/census/private_census.csv",
        mock_data_path="./data/census/mock_census.csv",
    )
    rds_client = RDSClient(host=TEST_EMAIL)

    future = rds_client.dataset.create(dataset)
    logger.debug(f"future = {future}")
    try:
        logger.debug(f"Waiting for the response: {future.wait(timeout=2)}")
    except Exception as e:
        logger.error(e)
