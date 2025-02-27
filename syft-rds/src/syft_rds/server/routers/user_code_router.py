from syft_rds.models.models import (
    GetAllRequest,
    GetOneRequest,
    UserCode,
    UserCodeCreate,
    UserCodeUpdate,
)
from syft_rds.server.router import RPCRouter
from syft_rds.store import RDSStore
from syft_core import Client

user_code_router = RPCRouter()
# TODO: add DI and remove globals
user_code_store = RDSStore(schema=UserCode, client=Client.load())


@user_code_router.on_request("/create")
def create_user_code(create_request: UserCodeCreate) -> UserCode:
    user_code = create_request.to_item()
    return user_code_store.create(user_code)


@user_code_router.on_request("/get_one")
def get_user_code(request: GetOneRequest) -> UserCode:
    return user_code_store.read(request.uid)


@user_code_router.on_request("/get_all")
def get_all_user_codes(request: GetAllRequest) -> list[UserCode]:
    return user_code_store.list_all()


@user_code_router.on_request("/update")
def update_user_code(update_request: UserCodeUpdate) -> UserCode:
    existing_item = user_code_store.read(update_request.uid)
    updated_item = update_request.apply_to(existing_item)
    return user_code_store.update(updated_item)
