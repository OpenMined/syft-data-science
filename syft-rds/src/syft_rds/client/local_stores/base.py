from typing import TYPE_CHECKING, ClassVar, Generic, List, Type, TypeVar

from pydantic import TypeAdapter
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

        self._field_validators = self._make_field_validators()

    def _make_field_validators(self) -> dict:
        """
        Create a dictionary of field_name: TypeAdapter for each field in the schema.
        These can be used to validate and convert field values to the correct type, required when querying the store.
        """
        return {
            field_name: TypeAdapter(field_info.annotation)
            for field_name, field_info in self.SCHEMA.model_fields.items()
        }

    def register_client_id(self, item: T) -> T:
        if isinstance(item, BaseSchema):
            item._register_client_id_recursive(self.config.client_id)
        return item

    def create(self, item: CreateT) -> T:
        raise NotImplementedError

    def update(self, item: UpdateT) -> T:
        raise NotImplementedError

    def get_one(self, request: GetOneRequest) -> T:
        # TODO use same logic for datasets (e.g. get_by_name == get_one(GetOneRequest(filters={"name": name}))
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

    def _coerce_field_types(self, filters: dict) -> dict:
        """
        If possible, convert filter values to the correct type for the schema.
        e.g. convert str to UUID, or str to Enum, etc.
        """
        # TODO move filter type coercion to RDSStore to avoid duplication serverside
        resolved_filters = {}
        for filter_name, filter_value in filters.items():
            validator = self._field_validators.get(filter_name, None)
            if validator is None:
                # Cannot infer type, leave it in the original form
                resolved_filters[filter_name] = filter_value
                continue
            try:
                type_adapter = self._field_validators[filter_name]
                validated_value = type_adapter.validate_python(filter_value)
                resolved_filters[filter_name] = validated_value
            except Exception:
                # Cannot convert to the correct type, leave it in the original form
                # logger.exception(
                #     f"Could not convert filter value {filter_value} to {field_info.annotation} for field {filter_name}"
                # )
                resolved_filters[filter_name] = filter_value
        return resolved_filters

    def _sort_results(self, items: List[T], order_by: str, sort_order: str) -> List[T]:
        return sorted(
            items,
            key=lambda x: getattr(x, order_by, None),
            reverse=sort_order == "desc",
        )

    def get_all(self, request: GetAllRequest) -> List[T]:
        # TODO use same logic for datasets
        # TODO move this logic to RDSStore and give RDSClient direct access to store instead of this in-between layer.
        # TODO Merge store get/query/search methods to get_one and get_all? pysyft does the same: https://github.com/OpenMined/PySyft/blob/dev/packages/syft/src/syft/store/db/stash.py#L522
        # Because: logic is needed both clientside and server side
        filters = self._coerce_field_types(request.filters)
        items = self.store.query(**filters)
        items = self._sort_results(items, request.order_by, request.sort_order)
        if request.offset:
            items = items[request.offset :]
        if request.limit:
            items = items[: request.limit]

        items = [self.register_client_id(item) for item in items]
        return items
