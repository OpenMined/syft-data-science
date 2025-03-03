from typing import TYPE_CHECKING

from syft_rds.client.connection import BlockingRPCConnection
from syft_rds.client.rpc_clients.base import RPCClientModule
from syft_rds.client.rpc_clients.dataset import DatasetRPCClient
from syft_rds.client.rpc_clients.jobs import JobRPCClient
from syft_rds.client.rpc_clients.runtime import RuntimeRPCClient
from syft_rds.client.rpc_clients.user_code import UserCodeRPCClient

if TYPE_CHECKING:
    from syft_rds.client.rds_client import RDSClientConfig


class RPCClient(RPCClientModule):
    def __init__(self, config: "RDSClientConfig", connection: BlockingRPCConnection):
        super().__init__(config, connection)
        self.jobs = JobRPCClient(self.config, self.connection)
        self.user_code = UserCodeRPCClient(self.config, self.connection)
        self.runtime = RuntimeRPCClient(self.config, self.connection)
        self.dataset = DatasetRPCClient(self.config, self.connection)
