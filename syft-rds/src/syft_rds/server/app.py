from types import MethodType

from loguru import logger
from syft_core import Client, SyftClientConfig
from syft_event import SyftEvents
from syft_rds.server.jobs import job_router
from syft_rds.server.router import RPCRouter
from syft_rds.server.user_code import user_code_router

APP_NAME = "RDS"


def create_app(client: Client | None = None) -> SyftEvents:
    rds_app = SyftEvents(app_name=APP_NAME, client=client)

    @rds_app.on_request("/info")
    def info() -> dict:
        return {"app_name": APP_NAME}

    def include_router(self, router: RPCRouter, *, prefix: str = "") -> None:
        for endpoint, func in router.routes.items():
            endpoint_with_prefix = f"{prefix}{endpoint}"
            _ = self.on_request(endpoint_with_prefix)(func)
            logger.info(f"Added endpoint: {endpoint_with_prefix}")

    rds_app.include_router = MethodType(include_router, rds_app)

    rds_app.include_router(job_router, prefix="/job")
    rds_app.include_router(user_code_router, prefix="/user_code")

    return rds_app


rds_app = create_app()
