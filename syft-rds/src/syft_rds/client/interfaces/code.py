from syft_core import Client as SyftBoxClient

from syft_rds.client.interfaces.base import CRUDInterface


class CodeInterface(CRUDInterface):
    def __init__(self, host: str, client: SyftBoxClient):
        super().__init__(host, client, "code")
