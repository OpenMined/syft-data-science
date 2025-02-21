from typing import Optional

from syft_core import Client

from syft_rds.client.interfaces.base import CRUDInterface


class RuntimeInterface(CRUDInterface):
    def __init__(self, host: str, client: Optional[Client] = None):
        super().__init__(host, "dataset", client)
