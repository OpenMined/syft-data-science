from pathlib import Path
from typing import Type

from syft_rds.models.base import ItemBase

USER_FILES_DIR = "user_files"

USER_FILES_PERMISSION_TEMPLATE = """
rules:
- pattern: '**'
  access:
    read:
    - '{useremail}'
"""


class UserFileService:
    """Service for managing user file directories with proper permissions.
    Users have read access to all files in their directory, and no access to other users' files.

    General structure:
    USER_FILES_DIR/
    ├── {useremail}/
    │   ├── syft.pub.yaml  # Read permissions for the user {useremail}
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

    def _is_valid_dirname(self, user: str) -> str:
        if not user or user in {".", ".."}:
            raise ValueError("Invalid directory name: cannot be '.' or '..' or empty")
        disallowed = {"/", "\\", "\x00"}
        if any(char in user for char in disallowed):
            raise ValueError(f"Invalid directory name: contains one of {disallowed}")
        return user

    def dir_for_user(self, user: str) -> Path:
        """Get the user's file directory, creating it if it doesn't exist"""
        user = self._is_valid_dirname(user)
        user_dir = self.user_files_dir / user
        user_dir.mkdir(exist_ok=True, parents=True)
        perm_file_path = user_dir / "syft.pub.yaml"
        if not perm_file_path.exists():
            with perm_file_path.open("w") as perm_file:
                perm_file.write(USER_FILES_PERMISSION_TEMPLATE.format(useremail=user))
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
