from syft_event import SyftEvents

from syft_rds.models import (
    GetAllRequest,
    GetOneRequest,
    ItemList,
    Runtime,
    RuntimeCreate,
    RuntimeUpdate,
)
from syft_rds.server.router import RPCRouter
from syft_rds.store import YAMLStore

runtime_router = RPCRouter()


@runtime_router.on_request("/create")
def create_runtime(create_request: RuntimeCreate, app: SyftEvents) -> Runtime:
    new_runtime = create_request.to_item()
    runtime_store: YAMLStore[Runtime] = app.state["runtime_store"]
    return runtime_store.create(new_runtime)


@runtime_router.on_request("/get_one")
def get_runtime(request: GetOneRequest, app: SyftEvents) -> Runtime:
    runtime_store: YAMLStore[Runtime] = app.state["runtime_store"]
    filters = request.filters
    if request.uid is not None:
        filters["uid"] = request.uid
    item = runtime_store.get_one(**filters)
    if item is None:
        raise ValueError(f"No runtime found with filters {filters}")
    return item


@runtime_router.on_request("/get_all")
def get_all_runtimes(req: GetAllRequest, app: SyftEvents) -> ItemList[Runtime]:
    runtime_store: YAMLStore[Runtime] = app.state["runtime_store"]
    items = runtime_store.get_all(
        limit=req.limit,
        offset=req.offset,
        order_by=req.order_by,
        sort_order=req.sort_order,
        filters=req.filters,
    )
    return ItemList[Runtime](items=items)


@runtime_router.on_request("/update")
def update_runtime(update_request: RuntimeUpdate, app: SyftEvents) -> Runtime:
    runtime_store: YAMLStore[Runtime] = app.state["runtime_store"]
    existing_item = runtime_store.get_by_uid(update_request.uid)
    if existing_item is None:
        raise ValueError(f"Runtime with uid {update_request.uid} not found")
    updated_item = existing_item.apply_update(update_request)
    return runtime_store.update(updated_item.uid, updated_item)
