from syft_rds.client.rpc_client import RPCClient
from syft_rds.services.dataset.dataset_model import CreateDataset


class RDSClient:
    def __init__(self, host: str):
        self.rpc_client = RPCClient(host)
        self.jobs = JobsRDSClient(host, self.rpc_client)
        self.dataset = DatasetRDSClient(host, self.rpc_client)


class BaseRDSClient:
    def __init__(self, host: str, rpc_client: RPCClient):
        self.host = host
        self.rpc_client = rpc_client


class JobsRDSClient(BaseRDSClient):
    def submit(self, job_id):
        prompt = input(f"Are you sure you want to approve job {job_id}? (yes/no): ")
        if prompt.lower() != "yes":
            print("Job approval cancelled")
            return None

        print(f"Submitting job {job_id}")
        return self.rpc_client.jobs.create(job_id)


class DatasetRDSClient(BaseRDSClient):
    def create(self, dataset: CreateDataset):
        # client-facing part
        prompt = input(
            f"Are you sure you want to upload dataset '{dataset.name}'? (yes/no): "
        )
        if prompt.lower() != "yes":
            print("Job approval cancelled")
            return None

        # server-facing (RPC) call
        return self.rpc_client.dataset.create(dataset)


def connect(host: str) -> RDSClient:
    return RDSClient(host)
