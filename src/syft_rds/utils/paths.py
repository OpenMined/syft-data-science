from pathlib import Path

import syft_rds

RDS_REPO_PATH = Path(syft_rds.__file__).parent.parent.parent
RDS_NOTEBOOKS_PATH = RDS_REPO_PATH / "notebooks"
