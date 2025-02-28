from syft_rds.client.rpc_clients.base import CRUDRPCClient
from syft_rds.models.models import Runtime, RuntimeCreate, RuntimeUpdate


class RuntimeRPCClient(CRUDRPCClient[Runtime, RuntimeCreate, RuntimeUpdate]):
    MODULE_NAME = "runtime"
