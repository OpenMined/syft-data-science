from pathlib import Path

from syft_rds.client.rds_clients.base import RDSClientModule
from syft_rds.client.utils import PathLike
from syft_rds.models import (
    CustomFunction,
    CustomFunctionCreate,
)
from syft_rds.utils.zip_utils import zip_to_bytes


class CustomFunctionRDSClient(RDSClientModule[CustomFunction]):
    ITEM_TYPE = CustomFunction

    def submit(
        self,
        name: str,
        code_path: PathLike,
        readme_path: PathLike | None = None,
        entrypoint: str | None = None,
    ) -> CustomFunction:
        code_path = Path(code_path)
        readme_path = Path(readme_path) if readme_path else None
        if not code_path.exists():
            raise FileNotFoundError(f"Path {code_path} does not exist.")
        if readme_path and not readme_path.exists():
            raise FileNotFoundError(f"Readme file {readme_path} does not exist.")

        if code_path.is_dir():
            # TODO handle directories. scenarios:
            # 1. code is file, readme is file
            # 2. code is dir, readme is file somewhere else
            # 3. code is dir, readme is in the dir
            # For simplicity, we only support 1 for now.
            raise NotImplementedError("Only single files are supported.")

        entrypoint = code_path.name
        files_to_zip = [code_path]
        if readme_path:
            files_to_zip.append(readme_path)
        files_zipped = zip_to_bytes(
            files_or_dirs=files_to_zip,
        )

        custom_function_create = CustomFunctionCreate(
            name=name,
            files_zipped=files_zipped,
            entrypoint=entrypoint,
            readme_filename=readme_path.name if readme_path else None,
        )

        custom_function = self.rpc.custom_function.create(custom_function_create)

        return custom_function
