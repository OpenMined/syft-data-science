from syft_core import Client as SyftBoxClient
from uuid import uuid4

from syft_rds.client.interfaces.base import CRUDInterface
from syft_rds.services.jobs.job_models import JobCreate


class JobsInterface(CRUDInterface):
    def __init__(self, host: str, syftbox_client: SyftBoxClient):
        super().__init__(host, syftbox_client, "jobs")

    def submit(
        self,
        name: str,
        description: str | None = None,
        runtime: str | None = None,
        tags: list[str] | None = None,
    ):
        job_create = JobCreate(
            name=name,
            description=description,
            runtime=runtime,
            user_code_id=uuid4(),
            tags=tags,
        )
        return super()._create(job_create)
