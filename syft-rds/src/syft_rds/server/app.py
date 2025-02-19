from syft_event import SyftEvents

from syft_rds.server.jobs import job_router
from syft_rds.server.router import RPCRouter
from syft_rds.server.user_code import user_code_router

APP_NAME = "RDS"

box = SyftEvents(app_name=APP_NAME)


def include_router(router: RPCRouter, *, prefix: str = "") -> None:
    for endpoint, func in router.routes.items():
        endpoint_with_prefix = f"{prefix}{endpoint}"
        _ = box.on_request(endpoint_with_prefix)(func)


box.include_router = include_router

box.include_router(job_router, prefix="/job")
box.include_router(user_code_router, prefix="/usercode")
