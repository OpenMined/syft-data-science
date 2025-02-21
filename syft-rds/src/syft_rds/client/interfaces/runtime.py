from syft_core import Client

from syft_rds.client.interfaces.base import CRUDInterface


class RuntimeInterface(CRUDInterface):
    def __init__(self, host: str, client: Client):
        super().__init__(host, "dataset", client)
