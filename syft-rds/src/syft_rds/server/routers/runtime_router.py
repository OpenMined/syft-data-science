from syft_rds.models.models import (
    GetAllRequest,
    GetOneRequest,
    Runtime,
    RuntimeCreate,
    RuntimeUpdate,
)
from syft_rds.server.router import RPCRouter
from syft_rds.server.store import Store

# Runtime Router
runtime_router = RPCRouter()
runtime_store = Store[Runtime]()


@runtime_router.on_request("/create")
def create_runtime(create_request: RuntimeCreate) -> Runtime:
    new_runtime = create_request.to_item()
    return runtime_store.create(new_runtime)


@runtime_router.on_request("/get_one")
def get_runtime(request: GetOneRequest) -> Runtime:
    return runtime_store.get_by_uid(request.uid)


@runtime_router.on_request("/get_all")
def get_all_runtimes(request: GetAllRequest) -> list[Runtime]:
    return runtime_store.get_all()


@runtime_router.on_request("/update")
def update_runtime(update_request: RuntimeUpdate) -> Runtime:
    existing_item = runtime_store.get_by_uid(update_request.uid)
    updated_item = update_request.apply_to(existing_item)
    return runtime_store.update(updated_item)
