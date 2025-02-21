from typing import Optional

from syft_core import Client as SyftBoxClient

from syft_rds.client.interfaces import (
    DataInterface,
    JobsInterface,
)


class RDSClient:
    def __init__(self, host: str, syftbox_client: Optional[SyftBoxClient] = None):
        self.host = host
        self.syftbox_client = (
            syftbox_client if syftbox_client is not None else SyftBoxClient.load()
        )

        self.data = DataInterface(self.host, self.syftbox_client)
        self.jobs = JobsInterface(self.host, self.syftbox_client)
        # self.runtime = RuntimeInterface(self.host, self.syftbox_client)
        # self.code = CodeInterface(self.host, self.syftbox_client)
