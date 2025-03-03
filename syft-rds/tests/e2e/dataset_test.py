import asyncio

import pytest
from loguru import logger

from .conftest import Client, E2EContext, Server
from syft_rds.client.rds_client import RDSClient, init_session


def deployment_config():
    return {
        "e2e_name": "launch",
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
async def test_e2e_launch(e2e_context: E2EContext):
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
    logger.info("Clients initialized", do_rds_client.email, ds_rds_client.email)
