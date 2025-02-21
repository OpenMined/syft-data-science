from typing import Optional

from syft_core import Client as SyftBoxClient

from syft_rds.client.interfaces import (
    DatasetInterface,
    JobsInterface,
    RuntimeInterface,
    CodeInterface,
)


class RDSClient:
    def __init__(self, host: str, client: Optional[SyftBoxClient] = None):
        self.host = host
        self.syftbox_client = client if client is not None else SyftBoxClient.load()

        self.dataset = DatasetInterface(self.host, self.syftbox_client)
        self.runtime = RuntimeInterface(self.host, self.syftbox_client)
        self.code = CodeInterface(self.host, self.syftbox_client)
        self.jobs = JobsInterface(self.host, self.syftbox_client)
