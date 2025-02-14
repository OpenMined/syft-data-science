

from pathlib import Path
from pydantic import BaseModel
from syft_core import Client
from syft_event import SyftEvents
from syft_rds.models.base_model import Item


class BaseRPCContext(BaseModel):
    class Config:
        arbitrary_types_allowed = True
        
    client: Client
    box: SyftEvents
    
    
    def item_dir(self, item_type: type[Item]) -> Path:
        return self.client.my_datasite / "apps" / self.box.app_name / "items" / item_type.cls_name