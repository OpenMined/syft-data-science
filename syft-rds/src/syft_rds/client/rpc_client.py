from abc import ABC

from syft_rpc import rpc
from syft_core import Client


class RPCClient:
    """Manages all RPC communications with the server."""

    def __init__(self, host: str):
        self.host = host
        print(f"Connecting to {host}")
        self.jobs = JobRPCClient(host)
        self.dataset = DatasetRPCClient(host)


class BaseRPCClient(ABC):  # our API = RPCClient which , but what's the
    """
    Base class for all RPC modules that
    communicate with the server by sending RPC requests.
    Note that all basic CRUD operations below need to send RPC requests
    """

    def __init__(self, host: str):
        self.host = host
        self.syftbox_client = Client.load()

    def create(self, resource) -> bool:
        """Create a new resource."""
        pass

    def get(self, resource):
        """Get a specific resource."""
        pass

    def delete(self, resource) -> bool:
        """Delete a specific resource."""
        pass

    def update(self, resource, data) -> bool:
        """Update a specific resource."""
        pass


class JobRPCClient(BaseRPCClient):
    def create(self, job_id):
        print(f"Sending RPC request to {self.host} to create job {job_id}")


class DatasetRPCClient(BaseRPCClient):
    def create(self, dataset):
        rpc_url = rpc.make_url(self.host, "dataset", "create")
        print(
            f"Sending RPC request to {self.host} from {self.syftbox_client.email} to create dataset with name {dataset.name}"
        )
        print(f"rpc url: {rpc_url}")
        return rpc.send(
            client=self.syftbox_client,
            url=rpc_url,
            body=dataset,
        )

    def get_all(self, cache: bool = True):
        """
        Client choose to
            Make RPC call to get_all (with an option).
            Just do this locally
        """
        pass
