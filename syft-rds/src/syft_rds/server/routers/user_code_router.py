from syft_event import SyftEvents
from syft_rds.models.models import (
    GetAllRequest,
    GetOneRequest,
    ItemList,
    UserCode,
    UserCodeCreate,
    UserCodeUpdate,
)
from syft_rds.server.router import RPCRouter
from syft_rds.store import RDSStore

user_code_router = RPCRouter()


@user_code_router.on_request("/create")
def create_user_code(create_request: UserCodeCreate, app: SyftEvents) -> UserCode:
    user_code = create_request.to_item()
    user_code_store: RDSStore = app.state["user_code_store"]
    return user_code_store.create(user_code)


@user_code_router.on_request("/get_one")
def get_user_code(request: GetOneRequest, app: SyftEvents) -> UserCode:
    user_code_store: RDSStore = app.state["user_code_store"]
    return user_code_store.read(request.uid)


@user_code_router.on_request("/get_all")
def get_all_user_codes(request: GetAllRequest, app: SyftEvents) -> ItemList[UserCode]:
    user_code_store: RDSStore = app.state["user_code_store"]
    items = user_code_store.list_all()
    return ItemList[UserCode](items=items)


@user_code_router.on_request("/update")
def update_user_code(update_request: UserCodeUpdate, app: SyftEvents) -> UserCode:
    user_code_store: RDSStore = app.state["user_code_store"]
    existing_item = user_code_store.read(update_request.uid)
    updated_item = update_request.apply_to(existing_item)
    return user_code_store.update(updated_item)
