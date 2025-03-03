from syft_event import SyftEvents
from syft_rds.models.models import (
    GetAllRequest,
    GetOneRequest,
    ItemList,
    Runtime,
    RuntimeCreate,
    RuntimeUpdate,
)
from syft_rds.server.router import RPCRouter
from syft_rds.store import RDSStore

runtime_router = RPCRouter()


@runtime_router.on_request("/create")
def create_runtime(create_request: RuntimeCreate, app: SyftEvents) -> Runtime:
    new_runtime = create_request.to_item()
    runtime_store: RDSStore = app.state["runtime_store"]
    return runtime_store.create(new_runtime)


@runtime_router.on_request("/get_one")
def get_runtime(request: GetOneRequest, app: SyftEvents) -> Runtime:
    runtime_store: RDSStore = app.state["runtime_store"]
    return runtime_store.read(request.uid)


@runtime_router.on_request("/get_all")
def get_all_runtimes(request: GetAllRequest, app: SyftEvents) -> ItemList[Runtime]:
    runtime_store: RDSStore = app.state["runtime_store"]
    items = runtime_store.list_all()
    return ItemList[Runtime](items=items)


@runtime_router.on_request("/update")
def update_runtime(update_request: RuntimeUpdate, app: SyftEvents) -> Runtime:
    runtime_store: RDSStore = app.state["runtime_store"]
    existing_item = runtime_store.read(update_request.uid)
    updated_item = update_request.apply_to(existing_item)
    return runtime_store.update(updated_item)
