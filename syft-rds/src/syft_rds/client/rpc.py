from functools import partial
from typing import (
    TYPE_CHECKING,
    Callable,
    ClassVar,
    Generic,
    Optional,
    Type,
    TypeVar,
    Union,
)
from uuid import UUID

from syft_rpc import SyftResponse
from syft_rpc.rpc import BodyType

from syft_rds.client.connection import BlockingRPCConnection
from syft_rds.models.base import BaseSchema, BaseSchemaCreate, BaseSchemaUpdate
from syft_rds.models.models import (
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

if TYPE_CHECKING:
    from syft_rds.client.rds_client import RDSClientConfig


T = TypeVar("T", bound=BaseSchema)
CreateT = TypeVar("CreateT", bound=BaseSchemaCreate)
UpdateT = TypeVar("UpdateT", bound=BaseSchemaUpdate)


class RPCClientModule:
    def __init__(self, config: "RDSClientConfig", connection: BlockingRPCConnection):
        self.config = config
        self.connection = connection

        self.prefix = f"syft://{self.config.host}/api_data/{self.config.app_name}/rpc"

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


def register_client_id_on_object(res: BaseSchema, client_id: UUID) -> BaseSchema:
    res._register_client_id_recursive(client_id)
    return res


class CRUDRPCClient(RPCClientModule, Generic[T, CreateT, UpdateT]):
    MODULE_NAME: ClassVar[str]
    SCHEMA: ClassVar[type[T]]

    def __init__(
        self,
        config: "RDSClientConfig",
        connection: BlockingRPCConnection,
        post_response_callback: Callable | None = None,
    ):
        super().__init__(config, connection)
        self.post_response_callback = post_response_callback

    def create(self, item: CreateT) -> T:
        response = self._send(f"{self.MODULE_NAME}/create", item)
        response.raise_for_status()

        res = response.model(self.SCHEMA)
        if self.post_response_callback:
            res = self.post_response_callback(res)
        return res

    def get_one(self, request: GetOneRequest) -> T:
        response = self._send(f"{self.MODULE_NAME}/get_one", request)
        response.raise_for_status()

        res = response.model(self.SCHEMA)
        if self.post_response_callback:
            res = self.post_response_callback(res)
        return res

    def get_all(self, request: GetAllRequest) -> list[T]:
        response = self._send(f"{self.MODULE_NAME}/get_all", request)
        response.raise_for_status()

        item_list = response.model(ItemList[self.SCHEMA])
        if self.post_response_callback:
            item_list.items = [
                self.post_response_callback(item) for item in item_list.items
            ]
        return item_list.items

    def update(self, item: UpdateT) -> T:
        response = self._send(f"{self.MODULE_NAME}/update", item)
        response.raise_for_status()

        res = response.model(self.SCHEMA)
        if self.post_response_callback:
            res = self.post_response_callback(res)
        return res


class DatasetRPCClient(CRUDRPCClient[Dataset, DatasetCreate, DatasetUpdate]):
    MODULE_NAME = "dataset"
    SCHEMA = Dataset


class JobRPCClient(CRUDRPCClient[Job, JobCreate, JobUpdate]):
    MODULE_NAME = "job"
    SCHEMA = Job


class RuntimeRPCClient(CRUDRPCClient[Runtime, RuntimeCreate, RuntimeUpdate]):
    MODULE_NAME = "runtime"
    SCHEMA = Runtime


class UserCodeRPCClient(CRUDRPCClient[UserCode, UserCodeCreate, UserCodeUpdate]):
    MODULE_NAME = "user_code"
    SCHEMA = UserCode


class RPCClient(RPCClientModule):
    def __init__(self, config: "RDSClientConfig", connection: BlockingRPCConnection):
        super().__init__(config, connection)
        callback = partial(
            register_client_id_on_object, client_id=self.config.client_id
        )

        self.jobs = JobRPCClient(
            self.config, self.connection, post_response_callback=callback
        )
        self.user_code = UserCodeRPCClient(
            self.config, self.connection, post_response_callback=callback
        )
        self.runtime = RuntimeRPCClient(
            self.config, self.connection, post_response_callback=callback
        )
        self.dataset = DatasetRPCClient(
            self.config, self.connection, post_response_callback=callback
        )

        # Create lookup table for type-based access
        self._type_map = {
            Job: self.jobs,
            UserCode: self.user_code,
            Runtime: self.runtime,
            Dataset: self.dataset,
        }

    def for_type(self, type_: Type[BaseSchema]) -> CRUDRPCClient:
        if type_ not in self._type_map:
            raise ValueError(f"No client registered for type {type_}")
        return self._type_map[type_]

    def health(self, expiry: Optional[Union[str, int]] = None) -> dict:
        response: SyftResponse = self._send("/health", body=None, expiry=expiry)
        response.raise_for_status()

        return response.json()
