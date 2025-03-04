from uuid import UUID

from syft_rds.client.rds_clients.base import RDSClientModule
from syft_rds.models.models import (
    GetAllRequest,
    GetOneRequest,
    UserCode,
)


class UserCodeRDSClient(RDSClientModule):
    def get_all(self) -> list[UserCode]:
        return self.rpc.user_code.get_all(GetAllRequest())

    def get(self, uid: UUID) -> UserCode:
        return self.rpc.user_code.get_one(GetOneRequest(uid=uid))
