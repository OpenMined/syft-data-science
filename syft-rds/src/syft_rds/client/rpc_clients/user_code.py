from syft_rds.client.rpc_clients.base import CRUDRPCClient
from syft_rds.models.models import UserCode, UserCodeCreate, UserCodeUpdate


class UserCodeRPCClient(CRUDRPCClient[UserCode, UserCodeCreate, UserCodeUpdate]):
    MODULE_NAME = "user_code"
