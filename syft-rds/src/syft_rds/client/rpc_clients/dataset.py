from syft_rds.client.rpc_clients.base import CRUDRPCClient
from syft_rds.models.models import Dataset, DatasetCreate, DatasetUpdate


class DatasetRPCClient(CRUDRPCClient[Dataset, DatasetCreate, DatasetUpdate]):
    MODULE_NAME = "dataset"
    MODEL_TYPE = Dataset
