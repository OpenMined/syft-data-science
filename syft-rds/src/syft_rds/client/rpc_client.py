from abc import ABC


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
    HAVE TO MAKE RPC CALL
    """

    def __init__(self, host: str):
        self.host = host

    def create(self, resource_id: str) -> bool:
        """Create a new resource."""
        pass

    def get(self, resource_id: str):
        """Get a specific resource."""
        pass

    def delete(self, resource_id: str) -> bool:
        """Delete a specific resource."""
        pass

    def update(self, resource_id: str, data: dict) -> bool:
        """Update a specific resource."""
        pass


class JobRPCClient(BaseRPCClient):
    def create(self, job_id):
        print(f"Sending RPC request to {self.host} to create job {job_id}")


class DatasetRPCClient(BaseRPCClient):
    def create(self, dataset_name):
        print(
            f"Sending RPC request to {self.host} to create dataset with name {dataset_name}"
        )

    def get_al(self, cache: bool = True):
        """
        Client choose to
            Make RPC call to get_all (with an option).
            Just do this locally
        """
        pass
