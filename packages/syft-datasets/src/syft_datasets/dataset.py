from datetime import datetime, timezone
from pathlib import Path
from typing import ClassVar

import yaml
from pydantic import BaseModel, Field
from syft_core import Client as SyftBoxClient
from syft_core.types import PathLike, to_path
from syft_core.url import SyftBoxURL
from syft_notebook_ui.formatter_mixin import (
    ANSIPydanticFormatter,
    PydanticFormatter,
    PydanticFormatterMixin,
)


def _utcnow():
    return datetime.now(tz=timezone.utc)


class Dataset(BaseModel, PydanticFormatterMixin):
    __display_formatter__: ClassVar[PydanticFormatter] = ANSIPydanticFormatter()

    _syftbox_client: SyftBoxClient | None = None
    created_at: datetime = Field(default_factory=_utcnow)
    updated_at: datetime = Field(default_factory=_utcnow)

    name: str
    created_at: datetime
    summary: str | None = None
    tags: list[str] = []

    mock_url: SyftBoxURL
    readme_url: SyftBoxURL | None = None

    @property
    def owner(self) -> str:
        return self.mock_url.host

    @property
    def syftbox_client(self) -> SyftBoxClient:
        if self._syftbox_client is None:
            raise ValueError("SyftBox client is not set.")
        return self._syftbox_client

    def _url_to_path(self, url: SyftBoxURL) -> Path:
        return url.to_local_path(
            datasites_path=self.syftbox_client.datasites,
        )

    @property
    def readme_path(self) -> Path | None:
        if self.readme_url is None:
            return None
        return self._url_to_path(self.readme_url)

    @property
    def mock_dir(self) -> Path:
        return self._url_to_path(self.mock_url)

    @property
    def private_dir(self) -> Path:
        if self.syftbox_client.email != self.owner:
            raise ValueError(
                "Cannot access private data for a dataset owned by another user."
            )

        # TODO add 'private' to sb workspace
        private_datasets_dir = (
            self.syftbox_client.workspace.data_dir / "private" / "syft_datasets"
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

    def describe(self) -> None:
        from IPython.display import HTML, display
        from syft_notebook_ui.pydantic_html_repr import create_html_repr

        fields_to_include = ["name", "created_at", "summary", "tags"]

        paths_to_include = []
        try:
            paths_to_include.append("mock_dir")
        except Exception:
            fields_to_include.append("mock_url")

        try:
            private_dir = self.private_dir
            if private_dir.is_dir():
                paths_to_include.append(("private_dir", self.private_dir))
        except Exception:
            pass

        try:
            readme_path = self.readme_path
            if readme_path and readme_path.exists():
                paths_to_include.append(("readme_path", readme_path))
        except Exception:
            fields_to_include.append("readme_url")

        description = create_html_repr(
            obj=self,
            fields=fields_to_include,
            display_paths=paths_to_include,
        )

        display(HTML(description))
