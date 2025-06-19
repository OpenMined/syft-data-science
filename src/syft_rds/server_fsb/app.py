from contextlib import asynccontextmanager
from typing import Any

from fastsyftbox import FastSyftBox
from syft_core import Client as SyftBoxClient

from syft_rds import __version__
from syft_rds.models import Job, Runtime, UserCode
from syft_rds.server_fsb.routers.user_code_router import user_code_router
from syft_rds.server_fsb.user_file_service import UserFileService
from syft_rds.store.store import YAMLStore

APP_NAME = "RDS"


def _init_services(app: FastSyftBox) -> None:
    app_dir = app.syftbox_client.app_data(app.app_name)
    store_dir = app_dir / "store"

    # Stores
    app.state.job_store = YAMLStore[Job](item_type=Job, store_dir=store_dir)
    app.state.user_code_store = YAMLStore[UserCode](
        item_type=UserCode, store_dir=store_dir
    )
    app.state.runtime_store = YAMLStore[Runtime](item_type=Runtime, store_dir=store_dir)

    # Services
    app.state.user_file_service = UserFileService(app_dir=app_dir)


@asynccontextmanager
async def rds_lifespan(app: FastSyftBox) -> Any:
    _init_services(app)
    yield


def create_app(client: SyftBoxClient | None = None) -> FastSyftBox:
    syftbox_config = None
    if client is not None:
        syftbox_config = client.config

    rds_app = FastSyftBox(
        app_name=APP_NAME,
        syftbox_config=syftbox_config,
        lifespan=rds_lifespan,
    )

    @rds_app.get("/health")
    def health() -> dict:  # pragma: no cover
        return {"app_name": APP_NAME, "version": __version__}

    # rds_app.include_router(job_router, prefix="/job")
    rds_app.include_router(user_code_router, prefix="/user_code")
    # rds_app.include_router(runtime_router, prefix="/runtime")

    return rds_app


if __name__ == "__main__":
    import uvicorn

    app = create_app()
    uvicorn.run(app)
