from types import MethodType

import yaml
from loguru import logger
from syft_core import Client
from syft_event import SyftEvents

from syft_rds import __version__
from syft_rds.models import Job, Runtime, UserCode
from syft_rds.models.custom_function_models import CustomFunction
from syft_rds.server.router import RPCRouter
from syft_rds.server.routers.custom_function_router import custom_function_router
from syft_rds.server.routers.job_router import job_router
from syft_rds.server.routers.runtime_router import runtime_router
from syft_rds.server.routers.user_code_router import user_code_router
from syft_rds.server.services.public_file_service import PublicFileService
from syft_rds.server.services.user_file_service import UserFileService
from syft_rds.store.store import YAMLStore

APP_NAME = "RDS"
APP_INFO_FILE = "app.yaml"
APP_SYFTPERM = f"""
- path: '{APP_INFO_FILE}'
  permissions:
  - read
  user: '*'
"""


def _init_services(app: SyftEvents) -> None:
    # Stores
    store_dir = app.app_dir / "store"
    app.state["job_store"] = YAMLStore[Job](item_type=Job, store_dir=store_dir)
    app.state["user_code_store"] = YAMLStore[UserCode](
        item_type=UserCode, store_dir=store_dir
    )
    app.state["runtime_store"] = YAMLStore[Runtime](
        item_type=Runtime, store_dir=store_dir
    )
    app.state["custom_function_store"] = YAMLStore[CustomFunction](
        item_type=CustomFunction, store_dir=store_dir
    )

    # UserFileService handles files on syftbox only visible to one user
    app.state["user_file_service"] = UserFileService(app_dir=app.app_dir)
    # PublicFileService handles files on syftbox that are readable by everyone
    app.state["public_file_service"] = PublicFileService(app_dir=app.app_dir)


def _write_app_info(app: SyftEvents) -> None:
    perm_path = app.app_dir / "syftperm.yaml"
    perm_path.write_text(APP_SYFTPERM)

    app_info = {
        "app_name": app.app_name,
        "app_version": __version__,
    }
    app_info_path = app.app_dir / APP_INFO_FILE
    if app_info_path.exists():
        # Load and check if the fields are the same
        with app_info_path.open("r") as f:
            existing_info = yaml.safe_load(f)
            for key, new_value in app_info.items():
                existing_value = existing_info.get(key)
                if existing_value != new_value:
                    logger.warning(
                        f"App info file contains a different {key}: {existing_value}. Migrations are not supported."
                    )
    with app_info_path.open("w") as f:
        yaml.safe_dump(app_info, f)


def create_app(client: Client | None = None) -> SyftEvents:
    rds_app = SyftEvents(app_name=APP_NAME, client=client)

    @rds_app.on_request("/health")
    def health() -> dict:
        return {"app_name": APP_NAME, "version": __version__}

    def include_router(self, router: RPCRouter, *, prefix: str = "") -> None:
        for endpoint, func in router.routes.items():
            endpoint_with_prefix = f"{prefix}{endpoint}"
            _ = self.on_request(endpoint_with_prefix)(func)

    rds_app.include_router = MethodType(include_router, rds_app)

    rds_app.include_router(job_router, prefix="/job")
    rds_app.include_router(user_code_router, prefix="/user_code")
    rds_app.include_router(runtime_router, prefix="/runtime")
    rds_app.include_router(custom_function_router, prefix="/custom_function")

    _init_services(rds_app)
    _write_app_info(rds_app)

    return rds_app
