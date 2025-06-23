from pathlib import Path

from syft_rds.client.rds_clients.base import RDSClientModule
from syft_rds.client.utils import PathLike
from syft_rds.models import (
    UserCode,
    UserCodeCreate,
    UserCodeType,
)
from syft_rds.utils.zip_utils import zip_to_bytes


class UserCodeRDSClient(RDSClientModule[UserCode]):
    ITEM_TYPE = UserCode

    def create(
        self,
        code_path: PathLike,
        name: str | None = None,
        entrypoint: str | None = None,
    ) -> UserCode:
        code_path = Path(code_path)
        if not code_path.exists():
            raise FileNotFoundError(f"Path {code_path} does not exist.")

        if code_path.is_dir():
            code_type = UserCodeType.FOLDER

            # Entrypoint is required for folder-type code
            if not entrypoint:
                raise ValueError("Entrypoint should be provided for folder code.")

            # Validate that the entrypoint exists within the folder
            if not (code_path / entrypoint).exists():
                raise FileNotFoundError(
                    f"Entrypoint {entrypoint} does not exist in {code_path}."
                )
            files_zipped = zip_to_bytes(files_or_dirs=[code_path], base_dir=code_path)
        else:
            code_type = UserCodeType.FILE

            # For file-type code, the entrypoint is the file name
            entrypoint = entrypoint or code_path.name

            files_zipped = zip_to_bytes(files_or_dirs=code_path)

        user_code_create = UserCodeCreate(
            name=name,
            files_zipped=files_zipped,
            code_type=code_type,
            entrypoint=entrypoint,
        )

        user_code = self.rpc.user_code.create(user_code_create)

        return user_code
