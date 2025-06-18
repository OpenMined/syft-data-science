import shutil
from pathlib import Path
from typing import Final, Type

from syft_rds.client.local_stores.base import CRUDLocalStore
from syft_rds.models.models import Job, JobCreate, JobUpdate


class JobLocalStore(CRUDLocalStore[Job, JobCreate, JobUpdate]):
    ITEM_TYPE: Final[Type[Job]] = Job

    def share_result_files(self, job: Job, job_output_folder: Path) -> Path:
        """
        Share the results with the user by moving the output files from the job output folder (local filesystem)
        to the output folder on SyftBox.
        """

        syftbox_output_path = job.output_url.to_local_path(
            self.syftbox_client.datasites
        )
        if not syftbox_output_path.exists():
            syftbox_output_path.mkdir(parents=True)

        # Copy all contents from job_output_folder to the output path
        for item in job_output_folder.iterdir():
            if item.is_file():
                shutil.copy2(item, syftbox_output_path)
            elif item.is_dir():
                shutil.copytree(
                    item,
                    syftbox_output_path / item.name,
                    dirs_exist_ok=True,
                )

        return syftbox_output_path
