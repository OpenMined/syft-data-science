from syft_rds.client.rpc_client import RPCClient


class RDSClient:
    def __init__(self, host: str):
        self.rpc_client = RPCClient(host)
        self.jobs = JobsService(host, self.rpc_client)
        self.dataset = DatasetService(host, self.rpc_client)


class BaseService:
    def __init__(self, host: str, rpc_client: RPCClient):
        self.host = host
        self.rpc_client = rpc_client


class JobsService(BaseService):
    def submit(self, job_id):
        prompt = input(f"Are you sure you want to approve job {job_id}? (yes/no): ")
        if prompt.lower() != "yes":
            print("Job approval cancelled")
            return None

        print(f"Submitting job {job_id}")
        return self.rpc_client.jobs.create(job_id)


class DatasetService(BaseService):
    def create(self, dataset_name: str):
        print(f"creating dataset with name {dataset_name}")

        return self.rpc_client.dataset.create(dataset_name)


def connect(host: str) -> RDSClient:
    return RDSClient(host)


if __name__ == "__main__":
    client: RDSClient = connect("rasswanth@openmined.org")
    client.jobs.submit("abc123")
    client.dataset.create("my very private dataset")
