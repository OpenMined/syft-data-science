from pathlib import Path
from typing import Type

from syft_rds.models.base import ItemBase

PUBLIC_FILES_PERMISSIONS = """
rules:
- pattern: '**'
  access:
    read:
    - '*'
"""


class PublicFileService:
    def __init__(self, app_dir: Path):
        """Service for managing files for the RDS app that everyone can read.

        General structure:
        APP_FILES_DIR/
        ├── syft.pub.yaml  # Read permissions for everyone
        ├── {item_type}/
        │   ├── {item_uid}/
        │   │   ├── public_file.txt

        Args:
            app_dir: The root application directory
        """
        self.app_dir = app_dir
        self.public_files_dir = app_dir / "public_files"
        self._init_public_files_dir()

    def _init_public_files_dir(self) -> None:
        self.public_files_dir.mkdir(exist_ok=True)
        perm_path = self.public_files_dir / "syft.pub.yaml"
        perm_path.write_text(PUBLIC_FILES_PERMISSIONS)

    def dir_for_type(self, type_: Type[ItemBase]) -> Path:
        if not issubclass(type_, ItemBase):
            raise ValueError(f"Type {type_} must be a subclass of ItemBase")
        type_dir = self.public_files_dir / type_.__name__
        type_dir.mkdir(exist_ok=True)
        return type_dir

    def dir_for_item(self, item: ItemBase) -> Path:
        type_dir = self.dir_for_type(type(item))
        item_dir = type_dir / str(item.uid)
        item_dir.mkdir(exist_ok=True)
        return item_dir
