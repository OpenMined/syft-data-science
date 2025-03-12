from syft_core import SyftBoxURL
from syft_event import SyftEvents
from syft_event.types import Request
from syft_rds.models.models import (
    GetAllRequest,
    GetOneRequest,
    ItemList,
    UserCode,
    UserCodeCreate,
    UserCodeUpdate,
)
from syft_rds.server.router import RPCRouter
from syft_rds.server.user_file_service import UserFileService
from syft_rds.store import RDSStore
from syft_rds.utils.zip_utils import extract_zip

user_code_router = RPCRouter()


@user_code_router.on_request("/create")
def create_user_code(
    create_request: UserCodeCreate, app: SyftEvents, request: Request
) -> UserCode:
    user_code_store: RDSStore = app.state["user_code_store"]
    user_file_service: UserFileService = app.state["user_file_service"]
    user = request.sender  # TODO auth

    create_request.name = create_request.name or "My Code"
    user_code = create_request.to_item(extra={"created_by": user})
    user_code_dir = user_file_service.dir_for_item(user=user, item=user_code)

    if create_request.files_zipped is not None:
        extract_zip(create_request.files_zipped, user_code_dir)

    user_code.dir_url = SyftBoxURL.from_path(user_code_dir, app.client.workspace)

    return user_code_store.create(user_code)


@user_code_router.on_request("/get_one")
def get_user_code(request: GetOneRequest, app: SyftEvents) -> UserCode:
    user_code_store: RDSStore = app.state["user_code_store"]
    return user_code_store.get_by_uid(request.uid)


@user_code_router.on_request("/get_all")
def get_all_user_codes(request: GetAllRequest, app: SyftEvents) -> ItemList[UserCode]:
    user_code_store: RDSStore = app.state["user_code_store"]
    items = user_code_store.list_all()
    return ItemList[UserCode](items=items)


@user_code_router.on_request("/update")
def update_user_code(update_request: UserCodeUpdate, app: SyftEvents) -> UserCode:
    user_code_store: RDSStore = app.state["user_code_store"]
    existing_item = user_code_store.get_by_uid(update_request.uid)
    updated_item = update_request.apply_to(existing_item)
    return user_code_store.update(updated_item)
