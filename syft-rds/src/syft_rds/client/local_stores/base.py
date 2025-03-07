from typing import TYPE_CHECKING, ClassVar, Generic, List, Type, TypeVar

from syft_core import Client as SyftBoxClient

from syft_rds.models.base import BaseSchema, BaseSchemaCreate, BaseSchemaUpdate
from syft_rds.models.models import (
    GetAllRequest,
    GetOneRequest,
)
from syft_rds.store import RDSStore

if TYPE_CHECKING:
    from syft_rds.client.rds_client import RDSClientConfig

T = TypeVar("T", bound=BaseSchema)
CreateT = TypeVar("CreateT", bound=BaseSchemaCreate)
UpdateT = TypeVar("UpdateT", bound=BaseSchemaUpdate)


class CRUDLocalStore(Generic[T, CreateT, UpdateT]):
    SCHEMA: ClassVar[Type[T]]

    def __init__(
        self,
        config: "RDSClientConfig",
        syftbox_client: SyftBoxClient,
    ):
        if not hasattr(self, "SCHEMA"):
            raise ValueError(f"{self.__class__.__name__} must define a SCHEMA.")

        self.config = config
        self.syftbox_client = syftbox_client
        self.store = RDSStore(
            schema=self.SCHEMA,
            client=self.syftbox_client,
            datasite=self.config.host,
        )

    def register_client_id(self, res: T) -> T:
        if isinstance(T, BaseSchema):
            res.register_client_id_recursively(self.config.client_id)
        return res

    def create(self, item: CreateT) -> T:
        raise NotImplementedError

    def update(self, item: UpdateT) -> T:
        raise NotImplementedError

    def get_one(self, request: GetOneRequest) -> T:
        # TODO implement get_all with limit + early return, to prevent loading all items on get_one
        res = self.get_all(GetAllRequest(filters=request.filters, limit=1))
        if len(res) == 0:
            filters_formatted: str = ", ".join(
                [f"{k}={v}" for k, v in request.filters.items()]
            )
            raise ValueError(
                f"No {self.SCHEMA.__name__} found with filters {filters_formatted}"
            )
        return res[0]

    def get_all(self, request: GetAllRequest) -> List[T]:
        # TODO move this logic to RDSStore and give RDSClient direct access to store instead of this in-between layer.
        # TODO Merge store get/query/search methods to get_one and get_all? pysyft does the same: https://github.com/OpenMined/PySyft/blob/dev/packages/syft/src/syft/store/db/stash.py#L522
        # Because: logic is needed both clientside and server side
        items = self.store.query(**request.filters)
        if request.offset:
            items = items[request.offset :]
        if request.limit:
            items = items[: request.limit]
        # TableList is a custom list subtype for pretty printing in Jupyter
        items = [self.register_client_id(item) for item in items]
        return items
