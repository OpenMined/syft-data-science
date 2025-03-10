from syft_rds.client.rds_clients.base import RDSClientModule
from syft_rds.models.models import (
    UserCode,
)


class UserCodeRDSClient(RDSClientModule[UserCode]):
    SCHEMA = UserCode
