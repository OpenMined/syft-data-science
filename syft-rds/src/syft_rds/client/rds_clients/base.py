from pydantic import BaseModel
from syft_core import Client as SyftBoxClient

from syft_rds.client.local_store import LocalStore
from syft_rds.client.rpc_client import RPCClient


class RDSClientConfig(BaseModel):
    host: str
    app_name: str = "RDS"
    default_runtime: str = "python"

    rpc_expiry: str = "5m"


class RDSClientModule:
    def __init__(
        self, config: RDSClientConfig, rpc_client: RPCClient, local_store: LocalStore
    ) -> None:
        self.config = config
        self.rpc = rpc_client
        self.local_store = local_store

    def set_default_runtime(self, runtime: str):
        self.config.default_runtime = runtime

    @property
    def host(self) -> str:
        return self.config.host

    @property
    def _syftbox_client(self) -> SyftBoxClient:
        return self.rpc.connection.sender_client

    @property
    def email(self) -> str:
        return self._syftbox_client.email

    @property
    def is_admin(self) -> bool:
        return self.host == self.email
