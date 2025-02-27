from syft_rds.client.rds_clients.base import RDSClientModule


class RuntimeRDSClient(RDSClientModule):
    def create(self, name: str) -> str:
        return self.rpc.runtime.create(name)

    def get_all(self) -> list[str]:
        return self.rpc.runtime.get_all()
