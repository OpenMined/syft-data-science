from syft_rds.models.base import ItemBase, ItemBaseCreate, ItemBaseUpdate
from syft_runtimes.models import (
    Runtime as BaseRuntime,
    RuntimeCreate as BaseRuntimeCreate,
    RuntimeUpdate as BaseRuntimeUpdate,
)


class Runtime(BaseRuntime, ItemBase):
    """RDS Runtime model that combines syft_runtimes.Runtime with RDS ItemBase functionality"""

    __schema_name__ = "runtime"
    __table_extra_fields__ = [
        "name",
        "kind",
    ]


class RuntimeCreate(ItemBaseCreate[Runtime], BaseRuntimeCreate):
    """RDS RuntimeCreate model that combines syft_runtimes.RuntimeCreate with RDS ItemBaseCreate functionality"""

    pass


class RuntimeUpdate(ItemBaseUpdate[Runtime], BaseRuntimeUpdate):
    """RDS RuntimeUpdate model that combines syft_runtimes.RuntimeUpdate with RDS ItemBaseUpdate functionality"""

    pass
