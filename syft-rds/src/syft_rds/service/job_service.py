
from typing import ClassVar, Type
from uuid import UUID
from syft_rds.db.multi_file_db import MultiFileDBEngine
from syft_rds.models.action_object import ActionObject
from syft_rds.models.base_model import Item
from syft_rds.models.job import Job, JobCreate
from syft_rds.models.request import Request
from syft_rds.service.context import BaseRPCContext
from syft_rds.service.service import BaseService


class JobService(BaseService):
    
    item_type: ClassVar[Type[Item]] = Job
    
    @classmethod
    def from_context(cls, context: BaseRPCContext):
        return cls(db=MultiFileDBEngine.from_context(context, item_type=Job))
    
    def spawn_job(self, job: JobCreate) -> Job:
        job_item = job.to_item()
        # this will create a job file, which will be picked up by the runner
        return self.db.create_item(job_item)
    
    def list_jobs(self) -> list[Job]:
        return self.db.list_items()
    
    def get_job_result(self, context: BaseRPCContext, job_id: UUID) -> ActionObject:
        # TODO: do this via a service
        job = self.db.get_item(job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")
        
        action_object_db = MultiFileDBEngine.from_context(context, item_type=ActionObject)
        return action_object_db.get_item(job.result_id)
