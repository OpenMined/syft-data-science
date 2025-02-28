from typing import TYPE_CHECKING

from syft_core import Client as SyftBoxClient

from syft_rds.client.local_stores.base import CRUDLocalStore
from syft_rds.client.local_stores.jobs import JobLocalStore
from syft_rds.client.local_stores.user_code import UserCodeLocalStore
from syft_rds.client.local_stores.runtime import RuntimeLocalStore
from syft_rds.client.local_stores.dataset import DatasetLocalStore


if TYPE_CHECKING:
    from syft_rds.client.rds_client import RDSClientConfig


class LocalStore(CRUDLocalStore):
    def __init__(self, config: "RDSClientConfig", syftbox_client: SyftBoxClient):
        super().__init__(config, syftbox_client)
        self.jobs = JobLocalStore(self.config, self.syftbox_client)
        self.user_code = UserCodeLocalStore(self.config, self.syftbox_client)
        self.runtime = RuntimeLocalStore(self.config, self.syftbox_client)
        self.dataset = DatasetLocalStore(self.config, self.syftbox_client)
