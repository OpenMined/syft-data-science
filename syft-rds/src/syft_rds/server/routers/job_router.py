from syft_rds.models.models import (
    GetAllRequest,
    GetOneRequest,
    Job,
    JobCreate,
    JobUpdate,
)
from syft_rds.server.router import RPCRouter
from syft_rds.server.store import Store

job_router = RPCRouter()

# TODO add DI and remove globals
job_store = Store[Job]()


@job_router.on_request("/create")
def create_job(create_request: JobCreate) -> Job:
    # TODO should be injected into the function signature, ~ store: Provide[Store[Job]]
    new_item = create_request.to_item()
    return job_store.create(new_item)


@job_router.on_request("/get_one")
def get_job(request: GetOneRequest) -> Job:
    return job_store.get_by_uid(request.uid)


@job_router.on_request("/get_all")
def get_all_jobs(request: GetAllRequest) -> list[Job]:
    return job_store.get_all()


@job_router.on_request("/update")
def update_job(update_request: JobUpdate) -> Job:
    existing_item = job_store.get_by_uid(update_request.uid)
    updated_item = update_request.apply_to(existing_item)
    return job_store.update(updated_item)
