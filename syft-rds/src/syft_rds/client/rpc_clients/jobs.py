from syft_rds.client.rpc_clients.base import CRUDRPCClient
from syft_rds.models.models import Job, JobCreate, JobUpdate


class JobRPCClient(CRUDRPCClient[Job, JobCreate, JobUpdate]):
    MODULE_NAME = "job"
    MODEL_TYPE = Job
