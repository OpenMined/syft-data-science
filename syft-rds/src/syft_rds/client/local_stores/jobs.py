import shutil
from pathlib import Path
from typing import Final, Type

from syft_rds.client.local_stores.base import CRUDLocalStore
from syft_rds.models.models import Job, JobCreate, JobUpdate


class JobLocalStore(CRUDLocalStore[Job, JobCreate, JobUpdate]):
    SCHEMA: Final[Type[Job]] = Job

    def share_result_files(self, job: Job, job_output_folder: Path) -> None:
        """
        Share the results with the user by moving the output files from the job output folder (local filesystem)
        to the output folder on SyftBox.
        """

        output_path = self._syfturl_to_local_path(job.output_url)
        if not output_path.exists():
            output_path.mkdir(parents=True)

        # TODO add kwargs to ignore logs, outputs, etc.
        shutil.copytree(
            job_output_folder,
            output_path / job.name,
            dirs_exist_ok=True,
        )
