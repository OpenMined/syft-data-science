from pathlib import Path

import syft_rds

SYFT_DATASCIENCE_REPO_PATH = Path(syft_rds.__file__).parents[4]
SYFT_DATASCIENCE_NOTEBOOKS_PATH = SYFT_DATASCIENCE_REPO_PATH / "notebooks"
