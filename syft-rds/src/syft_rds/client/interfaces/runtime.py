from syft_rds.client.interfaces.base import CRUDInterface


class RuntimeInterface(CRUDInterface):
    def __init__(self, host):
        super().__init__(host, "runtime")
