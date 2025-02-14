from typing import ClassVar, Type
from syft_rds.db.dataset_db import DatasetQueryEngine
from syft_rds.models.base_model import Item
from syft_rds.models.dataset import Dataset
from syft_rds.service.context import BaseRPCContext
from syft_rds.service.service import BaseService

class DatasetService(BaseService):
    
    item_type: ClassVar[Type[Item]] = Dataset
    
    @classmethod
    def from_context(cls, context: BaseRPCContext):
        return cls(db=DatasetQueryEngine.from_context(context, item_type=Dataset))
    
    