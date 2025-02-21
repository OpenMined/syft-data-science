from loguru import logger
from abc import ABC

from syft_rpc import rpc
from syft_core.url import SyftBoxURL
from syft_rpc.rpc import BodyType
from syft_core import Client as SyftBoxClient

from syft_rds.consts import APP_NAME


class CRUDInterface(ABC):
    """A base interface for CRUD operations using SyftBox RPC.

    This class provides a standardized way to perform Create, Read, Update, and Delete
    operations through RPC calls using the SyftBox client.
    All its 4 basic CRUD methods are private, always perform RPC calls and
    should be called through the specific interfaces

    Args:
        host (str): The email of the remote datasite.
        client (SyftBoxClient): An instance of the SyftBox Client.
            If None, it will be loaded from the default configuration.
        resource_type (str): The type of resource being managed (e.g., "dataset", "jobs", "code"...).
    """

    def __init__(self, host: str, syftbox_client: SyftBoxClient, resource_type: str):
        self.host = host
        self.syftbox_client = syftbox_client
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

    def _get(self, body: BodyType):
        endpoint = f"{self.resource_type}/get"
        rpc_url: SyftBoxURL = rpc.make_url(self.host, APP_NAME, endpoint)
        logger.debug(f"sending RPC request to {rpc_url}")
        return rpc.send(
            client=self.syftbox_client,
            url=rpc_url,
            body=body,
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

    def _delete(self, body: BodyType):
        endpoint = f"{self.resource_type}/delete"
        rpc_url: SyftBoxURL = rpc.make_url(self.host, APP_NAME, endpoint)
        logger.debug(f"sending RPC request to {rpc_url}")
        return rpc.send(
            client=self.syftbox_client,
            url=rpc_url,
            body=body,
        )
