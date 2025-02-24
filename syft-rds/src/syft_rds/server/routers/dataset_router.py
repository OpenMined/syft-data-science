from syft_rds.models.models import (
    Dataset,
    DatasetCreate,
    DatasetUpdate,
    GetAllRequest,
    GetOneRequest,
)
from syft_rds.server.router import RPCRouter
from syft_rds.server.store import Store

# Dataset Router
dataset_router = RPCRouter()
dataset_store = Store[Dataset]()


@dataset_router.on_request("/create")
def create_dataset(create_request: DatasetCreate) -> Dataset:
    new_dataset = create_request.to_item()
    return dataset_store.create(new_dataset)


@dataset_router.on_request("/get_one")
def get_dataset(request: GetOneRequest) -> Dataset:
    return dataset_store.get_by_uid(request.uid)


@dataset_router.on_request("/get_all")
def get_all_datasets(request: GetAllRequest) -> list[Dataset]:
    return dataset_store.get_all()


@dataset_router.on_request("/update")
def update_dataset(update_request: DatasetUpdate) -> Dataset:
    existing_item = dataset_store.get_by_uid(update_request.uid)
    updated_item = update_request.apply_to(existing_item)
    return dataset_store.update(updated_item)
