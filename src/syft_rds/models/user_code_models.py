import base64
import enum
from pathlib import Path
from typing import Optional, TypeVar

from IPython.display import HTML, display
from pydantic import (
    field_serializer,
    field_validator,
)
from syft_core import SyftBoxURL

from syft_rds.display_utils.html_format import create_html_repr
from syft_rds.models.base import ItemBase, ItemBaseCreate, ItemBaseUpdate

T = TypeVar("T", bound=ItemBase)

MAX_USERCODE_ZIP_SIZE = 1  # MB


class UserCodeType(enum.Enum):
    FILE = "file"
    FOLDER = "folder"


class UserCode(ItemBase):
    __schema_name__ = "usercode"
    __table_extra_fields__ = [
        "name",
    ]

    name: str
    dir_url: SyftBoxURL | None = None
    code_type: UserCodeType
    entrypoint: str

    @property
    def local_dir(self) -> Path:
        if self.dir_url is None:
            raise ValueError("dir_url is not set")
        client = self._client
        return self.dir_url.to_local_path(
            datasites_path=client._syftbox_client.datasites
        )

    def describe(self) -> None:
        html_description = create_html_repr(
            obj=self,
            fields=[
                "uid",
                "created_by",
                "created_at",
                "updated_at",
                "name",
                "local_dir",
                "code_type",
                "entrypoint",
            ],
            display_paths=["local_dir"],
        )
        display(HTML(html_description))


class UserCodeCreate(ItemBaseCreate[UserCode]):
    name: Optional[str] = None
    files_zipped: bytes | None = None
    code_type: UserCodeType
    entrypoint: str

    @field_serializer("files_zipped")
    def serialize_to_str(self, v: bytes | None) -> str | None:
        # Custom serialize for zipped binary data
        if v is None:
            return None
        return base64.b64encode(v).decode()

    @field_validator("files_zipped", mode="before")
    @classmethod
    def deserialize_from_str(cls, v):
        # Custom deserialize for zipped binary data
        if isinstance(v, str):
            return base64.b64decode(v)
        return v

    @field_validator("files_zipped", mode="after")
    @classmethod
    def validate_code_size(cls, v: bytes) -> bytes:
        zip_size_mb = len(v) / 1024 / 1024
        if zip_size_mb > MAX_USERCODE_ZIP_SIZE:
            raise ValueError(
                f"Provided files too large: {zip_size_mb:.2f}MB. Max size is {MAX_USERCODE_ZIP_SIZE}MB"
            )
        return v


class UserCodeUpdate(ItemBaseUpdate[UserCode]):
    pass
