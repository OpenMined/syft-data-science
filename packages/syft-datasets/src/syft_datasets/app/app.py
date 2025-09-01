from contextlib import asynccontextmanager

from fastsyftbox import FastSyftBox
from syft_core import SyftClientConfig

from syft_datasets.app.routers import dataset_router, main_router
from syft_datasets.dataset_manager import SyftDatasetManager

APP_NAME = "syft-datasets"


@asynccontextmanager
async def lifespan(app: FastSyftBox):
    syftbox_client = app.syftbox_client
    dataset_manager = SyftDatasetManager(syftbox_client=syftbox_client)
    app.state.dataset_manager = dataset_manager

    yield


def create_app(syftbox_config: SyftClientConfig | None = None) -> FastSyftBox:
    """
    Create a FastSyftBox application with the given SyftBox client.
    """
    app = FastSyftBox(
        app_name=APP_NAME,
        syftbox_client=syftbox_config,
        lifespan=lifespan,
    )

    app.include_router(main_router)
    app.include_router(dataset_router)

    return app


if __name__ == "__main__":
    import uvicorn

    app = create_app()
    uvicorn.run(app)
