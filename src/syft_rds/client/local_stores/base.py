from typing import TYPE_CHECKING, ClassVar, Generic, List, Type, TypeVar

from syft_core import Client as SyftBoxClient

from syft_rds.display_utils.jupyter.types import TableList
from syft_rds.models import (
    GetAllRequest,
    GetOneRequest,
)
from syft_rds.models.base import ItemBase, ItemBaseCreate, ItemBaseUpdate
from syft_rds.store import YAMLStore

if TYPE_CHECKING:
    from syft_rds.client.rds_client import RDSClientConfig

T = TypeVar("T", bound=ItemBase)
CreateT = TypeVar("CreateT", bound=ItemBaseCreate)
UpdateT = TypeVar("UpdateT", bound=ItemBaseUpdate)


class CRUDLocalStore(Generic[T, CreateT, UpdateT]):
    ITEM_TYPE: ClassVar[Type[T]]

    def __init__(
        self,
        config: "RDSClientConfig",
        syftbox_client: SyftBoxClient,
    ):
        if not hasattr(self, "ITEM_TYPE"):
            raise ValueError(f"{self.__class__.__name__} must define a ITEM_TYPE.")

        self.config = config
        self.syftbox_client = syftbox_client
        self.store = YAMLStore[T](
            item_type=self.ITEM_TYPE,
            store_dir=self.store_dir,
        )

    @property
    def store_dir(self) -> str:
        app_dir = self.syftbox_client.app_data(
            self.config.app_name,
            datasite=self.config.host,
        )
        return app_dir / "store"

    def register_client_id(self, item: T) -> T:
        if isinstance(item, ItemBase):
            item._register_client_id_recursive(self.config.uid)
        return item

    def create(self, item: CreateT) -> T:
        raise NotImplementedError

    def update(self, item: UpdateT) -> T:
        raise NotImplementedError

    def get_one(self, request: GetOneRequest) -> T:
        filters = request.filters
        if request.uid is not None:
            filters["uid"] = request.uid

        res_or_none = self.store.get_one(**filters)
        if res_or_none is None:
            filters_formatted: str = ", ".join(
                [f"{k}={v}" for k, v in request.filters.items()]
            )
            raise ValueError(
                f"No {self.ITEM_TYPE.__name__} found with filters {filters_formatted}"
            )
        return self.register_client_id(res_or_none)

    def get_all(self, request: GetAllRequest) -> List[T]:
        items = self.store.get_all(
            limit=request.limit,
            offset=request.offset,
            order_by=request.order_by,
            sort_order=request.sort_order,
            filters=request.filters,
        )
        items = [self.register_client_id(item) for item in items]
        return TableList(items)
