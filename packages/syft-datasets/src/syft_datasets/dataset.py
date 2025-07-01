from pathlib import Path

import yaml
from pydantic import BaseModel
from syft_core import Client as SyftBoxClient
from syft_core.types import PathLike, to_path
from syft_core.url import SyftBoxURL


class Dataset(BaseModel):
    _syftbox_client: SyftBoxClient | None = None
    name: str
    summary: str | None = None
    tags: list[str] = []

    mock_url: SyftBoxURL

    @property
    def owner(self) -> str:
        return self.mock_url.host

    @property
    def mock_dir(self) -> Path:
        if self._syftbox_client is None:
            raise ValueError("SyftBox client is not set.")

        return self.mock_url.to_local_path(
            datasites_path=self._syftbox_client.datasites,
        )

    @property
    def private_dir(self) -> Path:
        """
        Returns the private path for the dataset.
        Raises FileNotFoundError if the private path does not exist.
        """
        if self._syftbox_client is None:
            raise ValueError("SyftBox client is not set.")

        if self._syftbox_client.email != self.owner:
            raise ValueError(
                "Cannot access private data for a dataset owned by another user."
            )

        # TODO add 'private' to sb workspace
        private_datasets_dir = (
            self._syftbox_client.workspace.data_dir / "private" / "syft_datasets"
        )

        return private_datasets_dir / self.name

    def save(self, filepath: PathLike) -> None:
        filepath = to_path(filepath)
        if not filepath.suffix == ".yaml":
            raise ValueError("Dataset metadata must be saved as a .yaml file.")

        if not filepath.parent.exists():
            filepath.parent.mkdir(parents=True, exist_ok=True)

        data = self.model_dump(mode="json")
        yaml_dump = yaml.safe_dump(data, indent=2, sort_keys=False)
        filepath.write_text(yaml_dump)

    @classmethod
    def load(
        cls,
        filepath: PathLike,
        syftbox_client: SyftBoxClient | None = None,
    ) -> "Dataset":
        filepath = to_path(filepath)
        if not filepath.exists():
            raise FileNotFoundError(f"Dataset metadata file not found: {filepath}")

        data = yaml.safe_load(filepath.read_text())
        return cls.model_validate(data, context={"_syftbox_client": syftbox_client})
