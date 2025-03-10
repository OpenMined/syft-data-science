from typing import TYPE_CHECKING

from syft_core import Client as SyftBoxClient

from syft_rds.client.local_stores.base import CRUDLocalStore
from syft_rds.client.local_stores.dataset import DatasetLocalStore
from syft_rds.client.local_stores.jobs import JobLocalStore
from syft_rds.client.local_stores.runtime import RuntimeLocalStore
from syft_rds.client.local_stores.user_code import UserCodeLocalStore
from syft_rds.models.base import BaseSchema
from syft_rds.models.models import Dataset, Job, Runtime, UserCode

if TYPE_CHECKING:
    from syft_rds.client.rds_client import RDSClientConfig


class LocalStore:
    def __init__(self, config: "RDSClientConfig", syftbox_client: SyftBoxClient):
        self.config = config
        self.syftbox_client = syftbox_client
        self.jobs = JobLocalStore(self.config, self.syftbox_client)
        self.user_code = UserCodeLocalStore(self.config, self.syftbox_client)
        self.runtime = RuntimeLocalStore(self.config, self.syftbox_client)
        self.dataset = DatasetLocalStore(self.config, self.syftbox_client)

    def for_type(self, type_: BaseSchema) -> CRUDLocalStore:
        if type_ == Job:
            return self.jobs
        elif type_ == UserCode:
            return self.user_code
        elif type_ == Runtime:
            return self.runtime
        elif type_ == Dataset:
            return self.dataset
        else:
            raise ValueError(f"No local store found for type {type_}.")
