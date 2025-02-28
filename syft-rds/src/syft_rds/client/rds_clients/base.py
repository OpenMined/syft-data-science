from pydantic import BaseModel

from syft_core import Client as SyftBoxClient

from syft_rds.client.rpc_client import RPCClient


class RDSClientConfig(BaseModel):
    host: str
    app_name: str = "RDS"
    default_runtime: str = "python"

    rpc_expiry: str = "5m"
    rpc_cache: bool = True


class RDSClientModule:
    def __init__(self, config: RDSClientConfig, rpc_client: RPCClient):
        self.config = config
        self.rpc = rpc_client

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
