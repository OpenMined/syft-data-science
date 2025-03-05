import asyncio
import pandas as pd

import pytest
from loguru import logger

from .conftest import Client, E2EContext, Server
from ..integration.dataset_test import _create_dataset, MOCK_DATA_PATH
from syft_rds.client.rds_client import RDSClient, init_session


def deployment_config():
    return {
        "e2e_name": "dataset_get",
        "server": Server(port=5001),
        "clients": [
            Client(name="data_owner", port=9090, server_port=5001),
            Client(name="data_scientist", port=9091, server_port=5001),
        ],
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "e2e_context", [deployment_config()], indirect=True, ids=["deployment"]
)
async def test_e2e_dataset_get(e2e_context: E2EContext):
    logger.info(f"Starting E2E '{e2e_context.e2e_name}'")
    e2e_context.reset_test_dir()
    await e2e_context.start_all()
    await asyncio.sleep(3)

    data_scientist = None
    data_owner = None
    for client in e2e_context.clients:
        if client.name == "data_owner":
            data_owner = client
        if client.name == "data_scientist":
            data_scientist = client
        assert client.datasite_dir.exists()
        assert client.api_dir.exists()
        assert client.public_dir.exists()

    do_rds_client: RDSClient = init_session(
        host=data_owner.email, syftbox_client_config_path=data_owner.config_path
    )
    ds_rds_client: RDSClient = init_session(
        host=data_owner.email, syftbox_client_config_path=data_scientist.config_path
    )
    assert do_rds_client.is_admin
    assert not ds_rds_client.is_admin
    logger.info("Clients initialized", do_rds_client.email, ds_rds_client.email)

    dataset_name = "Test"
    _create_dataset(do_rds_client, dataset_name)
    await asyncio.sleep(3)  # wait for the dataset to be synced

    test_dataset = ds_rds_client.dataset.get(dataset_name)
    logger.info("Dataset retrieved", test_dataset.name)
    assert test_dataset.get_description()
    assert test_dataset.describe()
    mock_df = pd.read_csv(test_dataset.get_mock_path() / "data.csv")
    assert mock_df.equals(pd.read_csv(MOCK_DATA_PATH / "data.csv"))

    with pytest.raises(Exception) as excinfo:
        test_dataset.get_private_path()
        logger.error(f"DS tries to access the private data raised {excinfo}")

    dataset_2_name = "Test2"
    _create_dataset(do_rds_client, dataset_2_name)
    await asyncio.sleep(3)

    datasets = ds_rds_client.dataset.get_all()
    assert len(datasets) == 2

    logger.info(f"Test passed for {e2e_context.e2e_name}")
