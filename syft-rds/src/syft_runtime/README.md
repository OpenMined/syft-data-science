# Syft Runtime

This repository demonstrates the usage of Syft Runtime, a tool for executing functions on datasets with configurable parameters in a secure, isolated environment.

## Usage Example

```bash
$ syft_runtime run ./example/ds/function1 ./example/do/dataset1/private_message.txt --timeout 10

Data mount path: /data/private_message.txt
╭────────────────────────────── Job Configuration ──────────────────────────────╮
│ Starting job                                                                  │
│ Function: example/ds/function1 → /code                                        │
│ Dataset:  example/do/dataset1/private_message.txt → /data/private_message.txt │
│ Output:   jobs/20250214_122758 → /output                                      │
│ Timeout:  10s                                                                 │
╰───────────────────────────────────────────────────────────────────────────────╯
```

### Command Breakdown

- `./example/ds/function1`: Path to the function to be executed, `main.py` is the default entrypoint at the moment
- `./example/do/dataset1/private_message.txt`: Input dataset path, could be a single file or a directory and
  accesible through the `DATA_PATH` environment variable in the function code.
- `--timeout 10`: Maximum execution time in seconds, after which the job will be killed.

# Runtime Environment

Data scientists can use the following environment variables to locate the input dataset `DATA_PATH` and process it and
save the outputs to the `OUTPUT_DIR` directory.

- OUTPUT_DIR: Directory to save the outputs and logs,
  owner's of the function can freely write to and read from this directory.
  Limited by adjustable container resource limits, such as file size, number of files, etc.
- DATA_PATH: Read only path to the input dataset. Could be a single file or a directory.
  Owner's of the function can read from this path.
- TIMEOUT: Maximum execution time in seconds, after which the job will be killed.

## Collecting outputs and logs

Outputs and logs are collected via `OUTPUT_DIR` environment variable

```
example_run/
├── output/     # Job output files
└── logs/       # Execution logs
    ├── stdout.log
    └── stderr.log
```

## Features

- Secure execution environment:
  - Isolated Docker container with no network access
  - Read-only root filesystem
  - Dropped system capabilities
  - Limited system resources
  - Restricted user permissions
  - Timeouts are enforced
  - Packages available in the container are limited to the ones listed in pyproject.toml
  - Outputs and logs are collected via `OUTPUT_DIR` environment variable
