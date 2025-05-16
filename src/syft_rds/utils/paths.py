import syft_rds
from pathlib import Path

RDS_REPO_PATH = Path(syft_rds.__file__).parent.parent.parent.parent
RDS_NOTEBOOKS_PATH = RDS_REPO_PATH / "notebooks"
