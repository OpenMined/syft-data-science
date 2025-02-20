from typing_extensions import Union

from uuid import UUID

from syft_rds.client.interfaces.base import CRUDInterface
from syft_rds.services.dataset.dataset_model import CreateDataset, UpdateDataset


class DatasetInterface(CRUDInterface):
    def __init__(self, host: str):
        super().__init__(host, "dataset")

    def create(self, dataset: CreateDataset):
        return super()._create(dataset)

    def get(self, dataset_id: Union[str, UUID]):
        return super()._get(str(dataset_id))

    def delete(self, dataset_id: Union[str, UUID]):
        return super()._delete(str(dataset_id))

    def update(self, update_dataset: UpdateDataset):
        return super()._update(update_dataset)
