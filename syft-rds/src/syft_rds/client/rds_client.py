from syft_rds.client.rpc_client import RPCClient, JobRPCClient


class RDSClient:
    def __init__(self, host: str):
        self.rpc_client = RPCClient(host)
        self.jobs = JobRDSClient(host, self.rpc_client)
        # self.dataset = DatasetRDSClient(host, self.rpc_client)


class RDSClientModule:
    def __init__(self, host: str, rpc_client: RPCClient):
        self.host = host
        self.rpc_client = rpc_client


class JobRDSClient(RDSClientModule):

    def submit(self, job_id):
        prompt = input(f"Are you sure you want to approve job {job_id}? (yes/no): ")
        if prompt.lower() != "yes":
            print("Job approval cancelled")
            return None
        
        print(f"Submitting job {job_id}")
        return self.rpc_client.jobs.create(job_id)


class DatasetRDSClient(RDSClientModule):
    pass


def connect(host: str) -> RDSClient:
    return RDSClient(host)


if __name__ == "__main__":
    client: RDSClient = connect("rasswanth@openmined.org")
    client.jobs.submit("abc123")