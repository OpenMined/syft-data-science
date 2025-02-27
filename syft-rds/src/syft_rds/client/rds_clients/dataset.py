from syft_rds.client.rds_clients.base import RDSClientModule


class DatasetRDSClient(RDSClientModule):
    def create(self, name: str) -> str:
        return self.rpc.dataset.create(name)

    def get_all(self) -> list[str]:
        return self.rpc.dataset.get_all()
