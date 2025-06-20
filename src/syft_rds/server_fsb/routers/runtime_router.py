from typing import Annotated

from fastapi import APIRouter, Depends

from syft_rds.models import (
    GetAllRequest,
    GetOneRequest,
    ItemList,
    Runtime,
    RuntimeCreate,
    RuntimeUpdate,
)
from syft_rds.server_fsb.dependencies import (
    get_current_user,
    get_runtime_store,
)
from syft_rds.store import YAMLStore

runtime_router = APIRouter()


@runtime_router.post("/create")
def create_runtime(
    create_request: RuntimeCreate,
    runtime_store: Annotated[YAMLStore[Runtime], Depends(get_runtime_store)],
    user: Annotated[str, Depends(get_current_user)],
) -> Runtime:
    new_runtime = create_request.to_item()
    return runtime_store.create(new_runtime)


@runtime_router.post("/get_one")
def get_runtime(
    req: GetOneRequest,
    runtime_store: Annotated[YAMLStore[Runtime], Depends(get_runtime_store)],
    user: Annotated[str, Depends(get_current_user)],
) -> Runtime:
    filters = req.filters
    if req.uid is not None:
        filters["uid"] = req.uid
    item = runtime_store.get_one(**filters)
    if item is None:
        raise ValueError(f"No runtime found with filters {filters}")
    return item


@runtime_router.post("/get_all")
def get_all_runtimes(
    req: GetAllRequest,
    runtime_store: Annotated[YAMLStore[Runtime], Depends(get_runtime_store)],
    user: Annotated[str, Depends(get_current_user)],
) -> ItemList[Runtime]:
    items = runtime_store.get_all(
        limit=req.limit,
        offset=req.offset,
        order_by=req.order_by,
        sort_order=req.sort_order,
        filters=req.filters,
    )
    return ItemList[Runtime](items=items)


@runtime_router.post("/update")
def update_runtime(
    update_request: RuntimeUpdate,
    runtime_store: Annotated[YAMLStore[Runtime], Depends(get_runtime_store)],
    user: Annotated[str, Depends(get_current_user)],
) -> Runtime:
    existing_item = runtime_store.get_by_uid(update_request.uid)
    if existing_item is None:
        raise ValueError(f"Runtime with uid {update_request.uid} not found")
    updated_item = existing_item.apply_update(update_request)
    return runtime_store.update(updated_item.uid, updated_item)
