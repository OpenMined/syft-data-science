from syft_core import Client as SyftBoxClient
from syft_event import SyftEvents

from syft_rds.client.connection import get_connection
from syft_rds.client.local_store import LocalStore
from syft_rds.client.rds_clients.base import RDSClientConfig, RDSClientModule
from syft_rds.client.rds_clients.dataset import DatasetRDSClient
from syft_rds.client.rds_clients.jobs import JobRDSClient
from syft_rds.client.rds_clients.runtime import RuntimeRDSClient
from syft_rds.client.rpc_client import RPCClient


def init_session(host: str, mock_server: SyftEvents | None = None) -> "RDSClient":
    """
    Initialize a session with the RDSClient.

    If `mock_server` is provided, a in-process RPC connection will be used.

    Args:
        host (str):
        mock_server (SyftEvents, optional): The server used for RPC communication, only used for mocking scenarios.
            Client will use a mock, in-process RPC connection if provided. Defaults to None.

    Returns:
        RDSClient: The RDSClient instance.
    """

    # Implementation note: All dependencies are initiated here so we can inject and mock them in tests.
    config = RDSClientConfig(host=host)
    syftbox_client = SyftBoxClient.load()

    use_mock = mock_server is not None
    connection = get_connection(syftbox_client, mock_server, mock=use_mock)
    rpc_client = RPCClient(config, connection)
    local_store = LocalStore(config, syftbox_client)
    return RDSClient(config, rpc_client, local_store)


class RDSClient(RDSClientModule):
    def __init__(
        self, config: RDSClientConfig, rpc_client: RPCClient, local_store: LocalStore
    ) -> None:
        super().__init__(config, rpc_client, local_store)
        self.jobs = JobRDSClient(self.config, self.rpc, self.local_store)
        self.runtime = RuntimeRDSClient(self.config, self.rpc, self.local_store)
        self.data = DatasetRDSClient(self.config, self.rpc, self.local_store)
