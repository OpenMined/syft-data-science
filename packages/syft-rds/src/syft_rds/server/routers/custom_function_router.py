from syft_core import SyftBoxURL
from syft_event import SyftEvents
from syft_event.types import Request

from syft_rds.models import (
    CustomFunction,
    CustomFunctionCreate,
    GetAllRequest,
    GetOneRequest,
    ItemList,
)
from syft_rds.server.router import RPCRouter
from syft_rds.server.services.public_file_service import PublicFileService
from syft_rds.store import YAMLStore
from syft_rds.utils.zip_utils import extract_zip

custom_function_router = RPCRouter()


@custom_function_router.on_request("/create")
def create_custom_function(
    create_request: CustomFunctionCreate, app: SyftEvents, request: Request
) -> CustomFunction:
    custom_function_store: YAMLStore[CustomFunction] = app.state[
        "custom_function_store"
    ]
    public_file_service: PublicFileService = app.state["public_file_service"]
    user = request.sender  # TODO auth

    custom_function = create_request.to_item(extra={"created_by": user})
    custom_function_dir = public_file_service.dir_for_item(item=custom_function)

    if create_request.files_zipped is not None:
        extract_zip(create_request.files_zipped, custom_function_dir)

    custom_function.dir_url = SyftBoxURL.from_path(
        custom_function_dir, app.client.workspace
    )

    return custom_function_store.create(custom_function)


@custom_function_router.on_request("/get_one")
def get_custom_function(request: GetOneRequest, app: SyftEvents) -> CustomFunction:
    custom_function_store: YAMLStore[CustomFunction] = app.state[
        "custom_function_store"
    ]
    filters = request.filters
    if request.uid is not None:
        filters["uid"] = request.uid
    item = custom_function_store.get_one(**filters)
    if item is None:
        raise ValueError(f"No CustomFunction found with filters {filters}")
    return item


@custom_function_router.on_request("/get_all")
def get_all_custom_functions(
    req: GetAllRequest, app: SyftEvents
) -> ItemList[CustomFunction]:
    custom_function_store: YAMLStore[CustomFunction] = app.state[
        "custom_function_store"
    ]
    items = custom_function_store.get_all(
        limit=req.limit,
        offset=req.offset,
        order_by=req.order_by,
        sort_order=req.sort_order,
        filters=req.filters,
    )
    return ItemList[CustomFunction](items=items)


@custom_function_router.on_request("/update")
def update_custom_function(update_request, app: SyftEvents) -> CustomFunction:
    custom_function_store: YAMLStore[CustomFunction] = app.state[
        "custom_function_store"
    ]
    existing_item = custom_function_store.get_by_uid(update_request.uid)
    if existing_item is None:
        raise ValueError(f"CustomFunction with uid {update_request.uid} not found")
    updated_item = existing_item.apply_update(update_request)
    return custom_function_store.update(updated_item.uid, updated_item)
