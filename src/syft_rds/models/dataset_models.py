from pathlib import Path
from uuid import UUID

from IPython.display import HTML, display
from pydantic import Field
from syft_core import SyftBoxURL

from syft_rds.display_utils.html_format import create_html_repr
from syft_rds.models.base import ItemBase, ItemBaseCreate, ItemBaseUpdate


class Dataset(ItemBase):
    __schema_name__ = "dataset"
    __table_extra_fields__ = [
        "name",
        "summary",
    ]

    name: str = Field(description="Name of the dataset.")
    private: SyftBoxURL = Field(description="Private Syft URL of the dataset.")
    mock: SyftBoxURL = Field(description="Mock Syft URL of the dataset.")
    summary: str | None = Field(description="Summary string of the dataset.")
    readme: SyftBoxURL | None = Field(description="REAMD.md Syft URL of the dataset.")
    tags: list[str] = Field(description="Tags for the dataset.")
    runtime_id: UUID | None = Field(
        default=None, description="ID of the default runtime for the dataset."
    )

    @property
    def mock_path(self) -> Path:
        return self.get_mock_path()

    @property
    def private_path(self) -> Path:
        return self.get_private_path()

    @property
    def readme_path(self) -> Path:
        return self.get_readme_path()

    def get_mock_path(self) -> Path:
        mock_path: Path = self.mock.to_local_path(
            datasites_path=self._syftbox_client.datasites
        )
        if not mock_path.exists():
            raise FileNotFoundError(f"Mock file not found at {mock_path}")
        return mock_path

    def get_private_path(self) -> Path:
        """
        Will always raise PermissionError for non-admin users since they
        don't have permission to access private data.
        """
        # Check if user is admin before attempting to access private path
        if not self._is_admin():
            raise PermissionError(
                f"You must be the datasite admin to access private data. "
                f"Your SyftBox email: '{self._syftbox_client.email}'. "
                f"Host email: '{self._client.config.host}'"
            )

        private_path: Path = self.private.to_local_path(
            datasites_path=self._syftbox_client.datasites
        )
        if not private_path.exists():
            raise FileNotFoundError(
                f"Private data not found at {private_path}. "
                f"Probably you don't have admin permission to the dataset."
            )
        return private_path

    def get_readme_path(self) -> Path:
        """
        Will always raise FileNotFoundError for non-admin since the
        private path will never by synced
        """
        readme_path: Path = self.readme.to_local_path(
            datasites_path=self._syftbox_client.datasites
        )
        if not readme_path.exists():
            raise FileNotFoundError(f"Readme file not found at {readme_path}")
        return readme_path

    def get_description(self) -> str:
        # read the description .md file
        with open(self.get_readme_path()) as f:
            return f.read()

    def describe(self):
        field_to_include = [
            "uid",
            "created_at",
            "updated_at",
            "name",
            "readme_path",
            "mock_path",
        ]
        try:
            # Only include private path if it exists and user has permission
            _ = self.private_path
            field_to_include.append("private_path")
        except (FileNotFoundError, PermissionError):
            pass

        description = create_html_repr(
            obj=self,
            fields=field_to_include,
            display_paths=["mock_path", "readme_path"],
        )

        display(HTML(description))

    def _is_admin(self) -> bool:
        """Check if the current user is admin by comparing email with host."""
        return self._client.email == self._client.host


class DatasetUpdate(ItemBaseUpdate[Dataset]):
    pass


class DatasetCreate(ItemBaseCreate[Dataset]):
    name: str = Field(description="Name of the dataset.")
    path: str = Field(description="Private path of the dataset.")
    mock_path: str = Field(description="Mock path of the dataset.")
    summary: str | None = Field(description="Summary string of the dataset.")
    description_path: str | None = Field(
        description="Path to the detailed REAMD.md of the dataset."
    )
    tags: list[str] | None = Field(description="Tags for the dataset.")
    runtime_id: UUID | None = Field(
        default=None, description="ID of the default runtime for the dataset."
    )
