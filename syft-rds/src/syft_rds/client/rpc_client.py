class RPCClient:
    """
    Implment the server-facing logic
    """

    def __init__(self, host: str):
        self.host = host
        print(f"Connecting to {host}")
        self.jobs = JobRPCClient(host)
        self.dataset = DatasetRPCClient(host)


class RPCClientModule:
    def __init__(self, host):
        self.host = host

    def create(self, job_id):
        pass

    def get(self):
        pass

    def get_all(self):
        pass

    def delete(self):
        pass

    def update(self):
        pass


class JobRPCClient(RPCClientModule):
    def create(self, job_id):
        print(f"Sending RPC request to {self.host} to create job {job_id}")


class DatasetRPCClient(RPCClientModule):
    def create(self, dataset_name):
        print(
            f"Sending RPC request to {self.host} to create dataset with name {dataset_name}"
        )
