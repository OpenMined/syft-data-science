from uuid import UUID

from syft_rds.models.models import Job, JobCreate
from syft_rds.server.router import RPCRouter
from syft_rds.server.store import Store

job_router = RPCRouter()


@job_router.on_request("/create")
def create_job(item: JobCreate) -> Job:
    # TODO should be injected into the function signature, ~ store: Provide[Store[Job]]
    store = Store[Job]()
    job = item.to_item()
    result = store.create(job)

    return result


@job_router.on_request("/get")
def get_job(uid: UUID) -> Job:
    store = Store[Job]()
    return store.get(uid)


@job_router.on_request("/get_all")
def get_all_jobs(
    order_by: str = "date_modified",
    order: str = "asc",
    limit: int | None = None,
    offset: int = 0,
) -> list[Job]:
    store = Store[Job]()
    return store.get_all(order_by, order, limit, offset)
