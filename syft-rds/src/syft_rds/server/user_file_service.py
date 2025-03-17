from pathlib import Path
from typing import Type

from syft_rds.models.base import ItemBase

USER_FILES_DIR = "user_files"
USER_FILES_PERMISSION = """
- path: '{useremail}/**'
  permissions:
  - read
  user: '*'
"""


class UserFileService:
    """Service for managing user file directories with proper permissions.
    Users have read access to all files in their directory, and no access to other users' files.

    General structure:
    USER_FILES_DIR/
    ├── syftperm.yaml
    ├── {useremail}/
    │   ├── {item_type}/
    │   │   ├── {item_uid}/

    Example: job outputs for uid "1234" for user 'alice@openmined.org' go in:
    USER_FILES_DIR/alice@openmined.org/Job/1234/

    To get the path for a specific user/job:
    user_file_service.dir_for_item(user="alice@openmined.org", item=job)
    """

    def __init__(self, app_dir: Path):
        """Initialize the user file service.

        Args:
            app_dir: The root application directory
        """
        self.app_dir = app_dir
        self.user_files_dir = app_dir / USER_FILES_DIR
        self._init_user_files_dir()

    def _init_user_files_dir(self) -> None:
        """Initialize the user files directory with proper permissions."""
        self.user_files_dir.mkdir(exist_ok=True)

        perm_path = self.user_files_dir / "syftperm.yaml"
        perm_path.write_text(USER_FILES_PERMISSION)

    def dir_for_user(self, user: str) -> Path:
        """Get the user's file directory, creating it if it doesn't exist"""
        user_dir = self.user_files_dir / user
        user_dir.mkdir(exist_ok=True, parents=True)
        return user_dir

    def dir_for_type(self, user: str, type_: Type[ItemBase]) -> Path:
        """Get the user's directory for a specific item type"""
        user_dir = self.dir_for_user(user)
        item_dir = user_dir / type_.__name__
        item_dir.mkdir(exist_ok=True)
        return item_dir

    def dir_for_item(self, user: str, item: ItemBase) -> Path:
        """Get the directory for a specific item instance"""
        type_dir = self.dir_for_type(user, type(item))
        item_dir = type_dir / str(item.uid)
        item_dir.mkdir(exist_ok=True)
        return item_dir
