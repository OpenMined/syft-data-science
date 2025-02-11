from syft_rds.db.db import BaseQueryEngine
from syft_rds.db.single_file_db import SingleFileDBEngine
from syft_rds.models.code import Code
from syft_rds.service.context import BaseRPCContext
from syft_rds.service.service import BaseService

class CodeService(BaseService):
    
    @classmethod
    def from_context(cls, context: BaseRPCContext):
        return cls(db=SingleFileDBEngine.from_context(context, item_type=Code))
    
    
def get_code_service() -> CodeService:
    return CodeService(db=BaseQueryEngine())