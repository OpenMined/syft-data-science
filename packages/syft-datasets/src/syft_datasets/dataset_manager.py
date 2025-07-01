import shutil
from pathlib import Path
from typing import Self

from syft_core import Client as SyftBoxClient
from syft_core import SyftBoxURL
from syft_core.types import PathLike, to_path
from typing_extensions import Literal

from syft_datasets.dataset import Dataset
from syft_datasets.file_utils import copy_dir_contents, is_empty_dir

FOLDER_NAME = "syft_datasets"
METADATA_FILENAME = "dataset.yaml"


class SyftDatasetManager:
    def __init__(self, syftbox_client: SyftBoxClient):
        self.syftbox_client = syftbox_client

    @classmethod
    def load(cls, config_path: PathLike | None = None) -> Self:
        syftbox_client = SyftBoxClient.load(config_path)
        return cls(syftbox_client=syftbox_client)

    def public_dir_for_datasite(self, datasite: str) -> Path:
        dir = self.syftbox_client.datasites / datasite / "public" / FOLDER_NAME
        dir.mkdir(parents=True, exist_ok=True)
        return dir

    @property
    def my_public_dir(self) -> Path:
        return self.public_dir_for_datasite(self.syftbox_client.email)

    @property
    def my_private_dir(self) -> Path:
        # TODO move workspace.private_dir to syftbox_client
        dir = self.syftbox_client.workspace.data_dir / "private" / FOLDER_NAME
        dir.mkdir(parents=True, exist_ok=True)
        return dir

    def get_mock_dataset_dir(self, dataset_name: str, datasite: str) -> Path:
        return self.public_dir_for_datasite(datasite) / dataset_name

    def get_private_dataset_dir(self, dataset_name: str) -> Path:
        return self.my_private_dir / dataset_name

    def _prepare_mock_data(self, dataset: Dataset, mock_path: Path) -> None:
        if not mock_path.exists():
            raise FileNotFoundError(f"Could not find mock data at {mock_path}")

        if (mock_path / METADATA_FILENAME).exists():
            raise ValueError(
                f"Mock data at {mock_path} contains reserved file {METADATA_FILENAME}. Please rename it and try again."
            )

        if not is_empty_dir(dataset.mock_dir):
            raise FileExistsError(
                f"Mock dir {dataset.mock_dir} already exists and is not empty."
            )

        copy_dir_contents(
            src=mock_path,
            dst=dataset.mock_dir,
            exists_ok=True,
        )

    def _prepare_private_data(
        self,
        dataset: Dataset,
        private_path: Path,
    ) -> None:
        private_dir = dataset.private_dir
        private_dir.mkdir(parents=True, exist_ok=True)
        if not is_empty_dir(private_dir):
            raise FileExistsError(
                f"Private dir {private_dir} already exists and is not empty."
            )

        if not private_path.exists():
            raise FileNotFoundError(f"Could not find private data at {private_path}")

        copy_dir_contents(
            src=private_path,
            dst=private_dir,
            exists_ok=True,
        )

    def _prepare_readme(self, dataset: Dataset, readme_path: Path | None) -> None:
        if readme_path is not None:
            if not readme_path.exists():
                raise FileNotFoundError(f"Could not find README at {readme_path}")
            shutil.copy2(readme_path, dataset.mock_dir / readme_path.name)

    def create(
        self,
        name: str,
        mock_path: PathLike,
        private_path: PathLike,
        summary: str | None = None,
        readme_path: Path | None = None,
        tags: list[str] | None = None,
    ) -> Dataset:
        mock_path = to_path(mock_path)
        private_path = to_path(private_path)
        readme_path = to_path(readme_path) if readme_path else None

        mock_dir = self.get_mock_dataset_dir(
            dataset_name=name,
            datasite=self.syftbox_client.email,
        )
        mock_url = SyftBoxURL.from_path(
            path=mock_dir,
            workspace=self.syftbox_client.workspace,
        )

        dataset = Dataset(
            name=name,
            mock_url=mock_url,
            summary=summary,
            tags=tags or [],
            _syftbox_client=self.syftbox_client,
        )

        self._prepare_mock_data(
            dataset=dataset,
            mock_path=mock_path,
        )

        self._prepare_private_data(
            dataset=dataset,
            private_path=private_path,
        )

        self._prepare_readme(
            dataset=dataset,
            readme_path=readme_path,
        )

        metadata_path = mock_dir / METADATA_FILENAME
        dataset.save(filepath=metadata_path)
        return dataset

    def get(self, dataset_name: str, datasite: str | None = None) -> Dataset:
        datasite = datasite or self.syftbox_client.email
        mock_dir = self.get_mock_dataset_dir(
            dataset_name=dataset_name,
            datasite=datasite,
        )

        if not mock_dir.exists():
            raise FileNotFoundError(f"Dataset {dataset_name} not found in {mock_dir}")
        metadata_path = mock_dir / METADATA_FILENAME
        if not metadata_path.exists():
            raise FileNotFoundError(f"Dataset metadata not found at {metadata_path}")

        return Dataset.load(
            filepath=metadata_path,
            syftbox_client=self.syftbox_client,
        )

    def get_all(
        self,
        datasite: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        order_by: str | None = None,
        sort_order: Literal["asc", "desc"] = "asc",
    ) -> list[Dataset]:
        all_datasets = []

        if datasite:
            datasites_to_check = [datasite]
        else:
            ds_folder = self.syftbox_client.datasites
            datasites_to_check = [ds.name for ds in ds_folder.iterdir() if ds.is_dir()]

        for datasite in datasites_to_check:
            public_datasets_dir = self.public_dir_for_datasite(datasite)
            if not public_datasets_dir.exists():
                continue
            for dataset_dir in public_datasets_dir.iterdir():
                if dataset_dir.is_dir() and (dataset_dir / METADATA_FILENAME).exists():
                    try:
                        dataset = Dataset.load(
                            filepath=dataset_dir / METADATA_FILENAME,
                            syftbox_client=self.syftbox_client,
                        )
                        all_datasets.append(dataset)
                    except Exception:
                        continue

        if order_by is not None:
            all_datasets.sort(
                key=lambda d: getattr(d, order_by),
                reverse=(sort_order.lower() == "desc"),
            )

        if offset is not None:
            all_datasets = all_datasets[offset:]
        if limit is not None:
            all_datasets = all_datasets[:limit]

        return all_datasets
