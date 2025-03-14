from syft_core import SyftBoxURL
from syft_event import SyftEvents
from syft_event.types import Request
from syft_rds.models.models import (
    GetAllRequest,
    GetOneRequest,
    ItemList,
    Job,
    JobCreate,
    JobUpdate,
)
from syft_rds.server.router import RPCRouter
from syft_rds.server.user_file_service import UserFileService
from syft_rds.store import RDSStore
from syft_rds.utils.name_generator import generate_name

job_router = RPCRouter()


@job_router.on_request("/create")
def create_job(create_request: JobCreate, app: SyftEvents, request: Request) -> Job:
    user = request.sender  # TODO auth
    job_store: RDSStore = app.state["job_store"]
    user_file_service: UserFileService = app.state["user_file_service"]

    create_request.name = create_request.name or generate_name()
    new_item = create_request.to_item()
    # Create the output directory, user_file_service makes it readable for the user who created the job
    job_output_dir = user_file_service.dir_for_item(
        user=user,
        item=new_item,
    )
    new_item.output_url = SyftBoxURL.from_path(job_output_dir, app.client.workspace)
    return job_store.create(new_item)


@job_router.on_request("/get_one")
def get_job(request: GetOneRequest, app: SyftEvents) -> Job:
    job_store: RDSStore = app.state["job_store"]
    return job_store.read(request.uid)


@job_router.on_request("/get_all")
def get_all_jobs(req: GetAllRequest, app: SyftEvents) -> ItemList[Job]:
    job_store: RDSStore = app.state["job_store"]
    items = job_store.list_all()
    return ItemList[Job](items=items)


@job_router.on_request("/update")
def update_job(update_request: JobUpdate, app: SyftEvents) -> Job:
    job_store: RDSStore = app.state["job_store"]
    existing_item = job_store.read(update_request.uid)
    updated_item = update_request.apply_to(existing_item)
    return job_store.update(updated_item.uid, updated_item)
