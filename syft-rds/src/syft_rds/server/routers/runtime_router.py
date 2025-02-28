from syft_core import Client
from syft_rds.models.models import (
    GetAllRequest,
    GetOneRequest,
    Runtime,
    RuntimeCreate,
    RuntimeUpdate,
)
from syft_rds.server.router import RPCRouter
from syft_rds.store import RDSStore

# Runtime Router
runtime_router = RPCRouter()
# TODO: add DI and remove globals
runtime_store = RDSStore(schema=Runtime, client=Client.load())


@runtime_router.on_request("/create")
def create_runtime(create_request: RuntimeCreate) -> Runtime:
    new_runtime = create_request.to_item()
    return runtime_store.create(new_runtime)


@runtime_router.on_request("/get_one")
def get_runtime(request: GetOneRequest) -> Runtime:
    return runtime_store.read(request.uid)


@runtime_router.on_request("/get_all")
def get_all_runtimes(request: GetAllRequest) -> list[Runtime]:
    return runtime_store.list_all()


@runtime_router.on_request("/update")
def update_runtime(update_request: RuntimeUpdate) -> Runtime:
    existing_item = runtime_store.read(update_request.uid)
    updated_item = update_request.apply_to(existing_item)
    return runtime_store.update(updated_item)
