from typing import (
    TYPE_CHECKING,
    ClassVar,
    Generic,
    Optional,
    Type,
    TypeVar,
    Union,
)

from syft_rpc import SyftResponse
from syft_rpc.rpc import BodyType

from syft_rds.client.connection import BlockingRPCConnection
from syft_rds.models import (
    Dataset,
    DatasetCreate,
    DatasetUpdate,
    GetAllRequest,
    GetOneRequest,
    ItemList,
    Job,
    JobCreate,
    JobUpdate,
    Runtime,
    RuntimeCreate,
    RuntimeUpdate,
    UserCode,
    UserCodeCreate,
    UserCodeUpdate,
)
from syft_rds.models.base import ItemBase, ItemBaseCreate, ItemBaseUpdate
from syft_rds.models.custom_function_models import (
    CustomFunction,
    CustomFunctionCreate,
    CustomFunctionUpdate,
)

if TYPE_CHECKING:
    from syft_rds.client.rds_client import RDSClientConfig


T = TypeVar("T", bound=ItemBase)
CreateT = TypeVar("CreateT", bound=ItemBaseCreate)
UpdateT = TypeVar("UpdateT", bound=ItemBaseUpdate)


class RPCClientModule:
    def __init__(self, config: "RDSClientConfig", connection: BlockingRPCConnection):
        self.config = config
        self.connection = connection

        self.prefix = f"syft://{self.config.host}/app_data/{self.config.app_name}/rpc"

    def _send(
        self, path: str, body: BodyType, expiry: Optional[Union[str, int]] = None
    ) -> SyftResponse:
        expiry = expiry or self.config.rpc_expiry
        if isinstance(expiry, int):
            expiry = f"{expiry}s"

        return self.connection.send(
            f"{self.prefix}/{path}",
            body,
            expiry=expiry,
            cache=False,
        )


class CRUDRPCClient(RPCClientModule, Generic[T, CreateT, UpdateT]):
    MODULE_NAME: ClassVar[str]
    ITEM_TYPE: ClassVar[type[T]]

    def register_client_id(self, item: T) -> T:
        if isinstance(item, ItemBase):
            item._register_client_id_recursive(self.config.uid)
        return item

    def create(self, item: CreateT) -> T:
        response = self._send(f"{self.MODULE_NAME}/create", item)
        response.raise_for_status()

        res = response.model(self.ITEM_TYPE)
        return self.register_client_id(res)

    def get_one(self, request: GetOneRequest) -> T:
        response = self._send(f"{self.MODULE_NAME}/get_one", request)
        response.raise_for_status()

        res = response.model(self.ITEM_TYPE)
        return self.register_client_id(res)

    def get_all(self, request: GetAllRequest) -> list[T]:
        response = self._send(f"{self.MODULE_NAME}/get_all", request)
        response.raise_for_status()

        item_list = response.model(ItemList[self.ITEM_TYPE])
        return [self.register_client_id(item) for item in item_list.items]

    def update(self, item: UpdateT) -> T:
        response = self._send(f"{self.MODULE_NAME}/update", item)
        response.raise_for_status()

        res = response.model(self.ITEM_TYPE)
        return self.register_client_id(res)


class DatasetRPCClient(CRUDRPCClient[Dataset, DatasetCreate, DatasetUpdate]):
    MODULE_NAME = "dataset"
    ITEM_TYPE = Dataset


class JobRPCClient(CRUDRPCClient[Job, JobCreate, JobUpdate]):
    MODULE_NAME = "job"
    ITEM_TYPE = Job


class RuntimeRPCClient(CRUDRPCClient[Runtime, RuntimeCreate, RuntimeUpdate]):
    MODULE_NAME = "runtime"
    ITEM_TYPE = Runtime


class UserCodeRPCClient(CRUDRPCClient[UserCode, UserCodeCreate, UserCodeUpdate]):
    MODULE_NAME = "user_code"
    ITEM_TYPE = UserCode


class CustomFunctionRPCClient(
    CRUDRPCClient[CustomFunction, CustomFunctionCreate, CustomFunctionUpdate]
):
    MODULE_NAME = "custom_function"
    ITEM_TYPE = CustomFunction


class RPCClient(RPCClientModule):
    def __init__(self, config: "RDSClientConfig", connection: BlockingRPCConnection):
        super().__init__(config, connection)

        self.job = JobRPCClient(self.config, self.connection)
        self.user_code = UserCodeRPCClient(self.config, self.connection)
        self.runtime = RuntimeRPCClient(self.config, self.connection)
        self.dataset = DatasetRPCClient(self.config, self.connection)
        self.custom_function = CustomFunctionRPCClient(self.config, self.connection)

        # Create lookup table for type-based access
        self._type_map = {
            Job: self.job,
            UserCode: self.user_code,
            Runtime: self.runtime,
            Dataset: self.dataset,
            CustomFunction: self.custom_function,
        }

    def for_type(
        self, type_: Type[T]
    ) -> CRUDRPCClient[T, ItemBaseCreate, ItemBaseUpdate]:
        if type_ not in self._type_map:
            raise ValueError(f"No client registered for type {type_}")
        return self._type_map[type_]

    def health(self, expiry: Optional[Union[str, int]] = None) -> dict:
        response: SyftResponse = self._send("/health", body=None, expiry=expiry)
        response.raise_for_status()

        return response.json()
