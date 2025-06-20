from typing import Annotated

from fastapi import APIRouter, Depends
from syft_core import Client as SyftBoxClient
from syft_core import SyftBoxURL

from syft_rds.models import (
    GetAllRequest,
    GetOneRequest,
    ItemList,
    Job,
    JobCreate,
    JobUpdate,
)
from syft_rds.server.user_file_service import UserFileService
from syft_rds.server_fsb.dependencies import (
    get_current_user,
    get_job_store,
    get_syftbox_client,
    get_user_file_service,
)
from syft_rds.store import YAMLStore
from syft_rds.utils.name_generator import generate_name

job_router = APIRouter()


@job_router.post("/create")
def create_job(
    create_request: JobCreate,
    job_store: Annotated[YAMLStore[Job], Depends(get_job_store)],
    user_file_service: Annotated[UserFileService, Depends(get_user_file_service)],
    user: Annotated[str, Depends(get_current_user)],
    syftbox_client: Annotated[SyftBoxClient, Depends(get_syftbox_client)],
) -> Job:
    create_request.name = create_request.name or generate_name()
    new_item = create_request.to_item(extra={"created_by": user})
    job_output_dir = user_file_service.dir_for_item(
        user=user,
        item=new_item,
    )
    new_item.output_url = SyftBoxURL.from_path(job_output_dir, syftbox_client.workspace)
    return job_store.create(new_item)


@job_router.post("/get_one")
def get_job(
    req: GetOneRequest,
    job_store: Annotated[YAMLStore[Job], Depends(get_job_store)],
    user: Annotated[str, Depends(get_current_user)],
) -> Job:
    filters = req.filters
    if req.uid is not None:
        filters["uid"] = req.uid
    item = job_store.get_one(**filters)
    if item is None:
        raise ValueError(f"No job found with filters {filters}")
    return item


@job_router.post("/get_all")
def get_all_jobs(
    req: GetAllRequest,
    job_store: Annotated[YAMLStore[Job], Depends(get_job_store)],
    user: Annotated[str, Depends(get_current_user)],
) -> ItemList[Job]:
    items = job_store.get_all(
        limit=req.limit,
        offset=req.offset,
        order_by=req.order_by,
        sort_order=req.sort_order,
        filters=req.filters,
    )
    return ItemList[Job](items=items)


@job_router.post("/update")
def update_job(
    update_request: JobUpdate,
    job_store: Annotated[YAMLStore[Job], Depends(get_job_store)],
    user: Annotated[str, Depends(get_current_user)],
) -> Job:
    existing_item = job_store.get_by_uid(update_request.uid)
    if existing_item is None:
        raise ValueError(f"Job with uid {update_request.uid} not found")
    updated_item = existing_item.apply_update(update_request)
    return job_store.update(updated_item.uid, updated_item)
