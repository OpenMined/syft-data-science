"""
Base class for interfaces that handles RPC requests
"""

from syft_rpc import rpc
from syft_core.url import SyftBoxURL
from syft_rds.consts import APP_NAME
from syft_rpc.rpc import BodyType
from syft_core import Client
from loguru import logger


class CRUDInterface:
    def __init__(self, host: str, resource_type: str):
        self.syftbox_client = Client.load()
        self.host = host
        self.resource_type = resource_type

    def _create(self, body: BodyType):
        endpoint = f"{self.resource_type}/create"
        rpc_url: SyftBoxURL = rpc.make_url(self.host, APP_NAME, endpoint)
        logger.debug(f"Sending RPC request with url {rpc_url}")
        return rpc.send(
            client=self.syftbox_client,
            url=rpc_url,
            body=body,
        )

    def _get(self, dataset_id: str):
        endpoint = f"{self.resource_type}/get"
        rpc_url: SyftBoxURL = rpc.make_url(self.host, APP_NAME, endpoint)
        logger.debug(f"sending RPC request to {rpc_url}")
        return rpc.send(
            client=self.syftbox_client,
            url=rpc_url,
            body=dataset_id,
        )

    def _update(self, body: BodyType):
        endpoint = f"{self.resource_type}/update"
        rpc_url: SyftBoxURL = rpc.make_url(self.host, APP_NAME, endpoint)
        logger.debug(f"Sending RPC request with url {rpc_url}")
        return rpc.send(
            client=self.syftbox_client,
            url=rpc_url,
            body=body,
        )

    def _delete(self, dataset_id: str):
        endpoint = f"{self.resource_type}/delete"
        rpc_url: SyftBoxURL = rpc.make_url(self.host, APP_NAME, endpoint)
        logger.debug(f"sending RPC request to {rpc_url}")
        return rpc.send(
            client=self.syftbox_client,
            url=rpc_url,
            body=dataset_id,
        )
