from pathlib import Path

from syft_rds.client.rds_clients.base import RDSClientModule
from syft_rds.client.utils import PathLike
from syft_rds.models import (
    CustomFunction,
    CustomFunctionCreate,
)
from syft_rds.models.custom_function_models import CustomFunctionType
from syft_rds.utils.zip_utils import zip_to_bytes


class CustomFunctionRDSClient(RDSClientModule[CustomFunction]):
    ITEM_TYPE = CustomFunction

    def submit(
        self,
        name: str,
        code_path: PathLike,
        entrypoint: str | None = None,
    ) -> CustomFunction:
        code_path = Path(code_path)
        if not code_path.exists():
            raise FileNotFoundError(f"Path {code_path} does not exist.")

        if code_path.is_dir():
            code_type = CustomFunctionType.FOLDER
            # Entrypoint is required for folder-type code
            if not entrypoint:
                raise ValueError("Entrypoint should be provided for folder code.")

            if not (code_path / entrypoint).exists():
                raise FileNotFoundError(
                    f"Entrypoint {entrypoint} does not exist in {code_path}."
                )
            files_zipped = zip_to_bytes(files_or_dirs=[code_path], base_dir=code_path)
        else:
            code_type = CustomFunctionType.FILE
            entrypoint = code_path.name
            files_zipped = zip_to_bytes(files_or_dirs=code_path)

        custom_function_create = CustomFunctionCreate(
            name=name,
            files_zipped=files_zipped,
            code_type=code_type,
            entrypoint=entrypoint,
        )

        custom_function = self.rpc.custom_function.create(custom_function_create)

        return custom_function
