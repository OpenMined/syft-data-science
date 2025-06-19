from fastapi import Request
from syft_core import Client as SyftBoxClient

from syft_rds.models import Job, Runtime, UserCode
from syft_rds.server_fsb.user_file_service import UserFileService
from syft_rds.store.store import YAMLStore


def get_job_store(request: Request) -> YAMLStore[Job]:
    return request.app.state.job_store


def get_user_code_store(request: Request) -> YAMLStore[UserCode]:
    return request.app.state.user_code_store


def get_runtime_store(request: Request) -> YAMLStore[Runtime]:
    return request.app.state.runtime_store


def get_user_file_service(request: Request) -> UserFileService:
    return request.app.state.user_file_service


def get_current_user(request: Request) -> str:
    try:
        return request.state.sender
    except AttributeError:
        return "anonymous"


def get_syftbox_client(request: Request) -> "SyftBoxClient":
    return request.app.syftbox_client
