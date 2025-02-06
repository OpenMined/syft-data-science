

from pydantic import BaseModel
from syft_core import Client
from syft_event import SyftEvents


class BaseRPCContext(BaseModel):
    class Config:
        arbitrary_types_allowed = True
        
    client: Client
    box: SyftEvents