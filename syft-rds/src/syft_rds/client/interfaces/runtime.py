from syft_core import Client as SyftBoxClient

from syft_rds.client.interfaces.base import CRUDInterface


class RuntimeInterface(CRUDInterface):
    def __init__(self, host: str, syftbox_client: SyftBoxClient):
        super().__init__(host, syftbox_client, "runtime")
