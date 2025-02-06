from pydantic import Field
from syft_rds.db.db import BaseDatabase
from syft_rds.service.service import BaseService

class DatasetService(BaseService):
    pass
    
    
def get_dataset_service() -> DatasetService:
    return DatasetService(db=BaseDatabase())