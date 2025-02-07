from pydantic import Field
from syft_rds.db.dataset_db import DatasetQueryEngine
from syft_rds.db.db import BaseQueryEngine
from syft_rds.service.context import BaseRPCContext
from syft_rds.service.service import BaseService

class DatasetService(BaseService):
    
    @classmethod
    def from_context(cls, context: BaseRPCContext):
        return cls(db=DatasetQueryEngine.from_context(context))
    
    
def get_dataset_service() -> DatasetService:
    return DatasetService(db=BaseQueryEngine())