import syft_rds as sy
from pathlib import Path
from loguru import logger


PROJECT_PATH = Path(__file__).parent.parent

DO1 = "rasswanth@openmined.org"
DS = "khoa@openmined.org"
DS_CONFIG = PROJECT_PATH / ".sim" / ".config" / f"{DS}.json"
DO1_CONFIG = PROJECT_PATH / ".sim" / ".config" / f"{DO1}.json"
FLWR_PROJECT_PATH = PROJECT_PATH / "experimentals" / "quickstart-pytorch"

logger.info(f"PROJECT_PATH: {PROJECT_PATH}")
logger.info(f"DS_CONFIG: {DS_CONFIG}")
logger.info(f"FLWR_PROJECT_PATH: {FLWR_PROJECT_PATH}")

DATASET_NAME = "cifar10"
DATASET_PRIVATE_PATH = FLWR_PROJECT_PATH.parent / "datasets" / "cifar10_part_1"
DATASET_MOCK_PATH = FLWR_PROJECT_PATH.parent / "datasets" / "cifar10_part_2"
README_PATH = FLWR_PROJECT_PATH / "README.md"


# --- DO ---
do = sy.init_session(host=DO1, syftbox_client_config_path=DO1_CONFIG)
assert do.host == DO1
assert do.email == DO1
assert do.is_admin

try:
    dataset = do.dataset.create(
        name=DATASET_NAME,
        summary=f"Partitioned {DATASET_NAME} dataset.",
        description_path=README_PATH,
        path=DATASET_PRIVATE_PATH,
        mock_path=DATASET_MOCK_PATH,
    )
    dataset.describe()
except Exception as e:
    logger.error(e)

assert len(do.datasets) == 1

# --- DS ---
do_client_1 = sy.init_session(host=DO1, syftbox_client_config_path=DS_CONFIG)
assert not do_client_1.is_admin
assert do_client_1.host == DO1
assert do_client_1.email == DS

# Job submission
job = do_client_1.jobs.submit(
    name="FLWR Experiment",
    dataset_name=do_client_1.datasets[0].name,
    description="Flower Federated Learning Experiment",
    user_code_path=FLWR_PROJECT_PATH,
    tags=["federated", "learning", "medical", "mnist"],
    # runtime="./dockerfile", # path to a local dockerfile
)

logger.info(f"Job submitted: {job}")
