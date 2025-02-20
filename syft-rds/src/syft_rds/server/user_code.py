from uuid import UUID

from syft_rds.models.models import UserCode, UserCodeCreate
from syft_rds.server.router import RPCRouter

user_code_router = RPCRouter()


@user_code_router.on_request("/create")
def create_usercode(item: UserCodeCreate) -> UserCode:
    return item.to_item()


@user_code_router.on_request("/get")
def get_usercode(uid: UUID) -> UserCode:
    pass


@user_code_router.on_request("/get_all")
def get_all_usercodes() -> list[UserCode]:
    pass
