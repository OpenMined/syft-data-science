from syft_rds.client.interfaces import (
    DatasetInterface,
    JobsInterface,
    RuntimeInterface,
    CodeInterface,
)


class RDSClient:
    def __init__(self, host: str):
        self.host = host
        self.dataset = DatasetInterface(host)
        self.runtime = RuntimeInterface(host)
        self.code = CodeInterface(host)
        self.jobs = JobsInterface(host)
