from typing import TYPE_CHECKING, Type, TypeVar

from syft_core import Client as SyftBoxClient

from syft_rds.client.local_stores.base import CRUDLocalStore
from syft_rds.client.local_stores.custom_function import CustomFunctionLocalStore
from syft_rds.client.local_stores.dataset import DatasetLocalStore
from syft_rds.client.local_stores.job import JobLocalStore
from syft_rds.client.local_stores.runtime import RuntimeLocalStore
from syft_rds.client.local_stores.user_code import UserCodeLocalStore
from syft_rds.models import Dataset, Job, Runtime, UserCode
from syft_rds.models.base import ItemBase, ItemBaseCreate, ItemBaseUpdate
from syft_rds.models.custom_function_models import CustomFunction

if TYPE_CHECKING:
    from syft_rds.client.rds_client import RDSClientConfig

T = TypeVar("T", bound=ItemBase)


class LocalStore:
    def __init__(self, config: "RDSClientConfig", syftbox_client: SyftBoxClient):
        self.config = config
        self.syftbox_client = syftbox_client
        self.job = JobLocalStore(self.config, self.syftbox_client)
        self.user_code = UserCodeLocalStore(self.config, self.syftbox_client)
        self.runtime = RuntimeLocalStore(self.config, self.syftbox_client)
        self.dataset = DatasetLocalStore(self.config, self.syftbox_client)
        self.custom_function = CustomFunctionLocalStore(
            self.config, self.syftbox_client
        )

        self._type_map = {
            Job: self.job,
            UserCode: self.user_code,
            Runtime: self.runtime,
            Dataset: self.dataset,
            CustomFunction: self.custom_function,
        }

    def for_type(
        self, type_: Type[T]
    ) -> CRUDLocalStore[T, ItemBaseCreate, ItemBaseUpdate]:
        if type_ not in self._type_map:
            raise ValueError(f"No local store found for type {type_}")
        return self._type_map[type_]
