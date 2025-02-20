from syft_rds.client.interfaces.dataset import DatasetInterface
from syft_rds.client.interfaces.jobs import JobsInterface
from syft_rds.client.interfaces.runtime import RuntimeInterface
from syft_rds.client.interfaces.code import CodeInterface


def connect(host: str) -> "RDSClient":
    return RDSClient(host)


class RDSClient:
    def __init__(self, host):
        self.host = host
        self.dataset = DatasetInterface(host)
        self.runtime = RuntimeInterface(host)
        self.code = CodeInterface(host)
        self.jobs = JobsInterface(host)
