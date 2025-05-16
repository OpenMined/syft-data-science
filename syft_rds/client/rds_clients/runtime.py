from syft_rds.client.rds_clients.base import RDSClientModule
from syft_rds.models.models import Runtime


class RuntimeRDSClient(RDSClientModule[Runtime]):
    ITEM_TYPE = Runtime
