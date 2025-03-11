from pathlib import Path

from syft_rds.client.rds_clients.base import RDSClientModule
from syft_rds.client.utils import PathLike
from syft_rds.models.models import (
    UserCode,
    UserCodeCreate,
)
from syft_rds.utils.zip_utils import zip_to_bytes


class UserCodeRDSClient(RDSClientModule[UserCode]):
    SCHEMA = UserCode

    def create(
        self,
        file_path: PathLike,
        name: str | None = None,
    ) -> UserCode:
        file_path = Path(file_path)
        if not file_path.is_file():
            raise ValueError(f"File not found: {file_path}")

        files_zipped = zip_to_bytes(files_or_dirs=[file_path])
        user_code_create = UserCodeCreate(
            name=name,
            files_zipped=files_zipped,
        )

        user_code = self.rpc.user_code.create(user_code_create)

        return user_code
