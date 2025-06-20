from syft_rds.client.local_stores.base import CRUDLocalStore
from syft_rds.models import CustomFunction, CustomFunctionCreate, CustomFunctionUpdate


class CustomFunctionLocalStore(
    CRUDLocalStore[CustomFunction, CustomFunctionCreate, CustomFunctionUpdate]
):
    ITEM_TYPE = CustomFunction
