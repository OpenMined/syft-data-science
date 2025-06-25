import json
import os
from pathlib import Path

DATA_DIR = os.environ["DATA_DIR"]
CODE_DIR = os.environ["CODE_DIR"]
OUTPUT_DIR = os.environ["OUTPUT_DIR"]


# Default paths for this custom function
user_params_file = Path(CODE_DIR) / "user_params.json"
output_file = Path(OUTPUT_DIR) / "result.json"


user_params_file = Path(CODE_DIR) / "user_params.json"
print("Loading user params from ", user_params_file.as_posix())
user_params = json.loads(user_params_file.read_text())

# This simple custom function just echoes the user parameters back to the output file.
result = user_params

output_file = Path(OUTPUT_DIR) / "result.json"
print(f"Writing result to {output_file}")
output_file.write_text(json.dumps(result, indent=2))
