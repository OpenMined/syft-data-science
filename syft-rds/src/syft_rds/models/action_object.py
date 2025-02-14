from typing import Any, ClassVar
from syft_rds.models.base_model import Item


class ActionObject(Item):
    cls_name: ClassVar[str] = "SyftActionObject"
    syft_action_data: Any
