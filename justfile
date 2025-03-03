# Guidelines for new commands
# - Start with a verb
# - Keep it short (max. 3 words in a command)
# - Group commands by context. Include group name in the command name.
# - Mark things private that are util functions with [private] or _var
# - Don't over-engineer, keep it simple.
# - Don't break existing commands
# - Run just --fmt --unstable after adding new commands

set dotenv-load := true

# ---------------------------------------------------------------------------------------------------------------------
# Private vars

_red := '\033[1;31m'
_cyan := '\033[1;36m'
_green := '\033[1;32m'
_yellow := '\033[1;33m'
_nc := '\033[0m'

# ---------------------------------------------------------------------------------------------------------------------
# Aliases

alias rj := run-jupyter

# ---------------------------------------------------------------------------------------------------------------------

@default:
    just --list

[group('utils')]
run-jupyter jupyter_args="":
    # uv sync

    uv run --frozen --with "jupyterlab" \
        jupyter lab {{ jupyter_args }}

build runtime:
    docker build -t syft_python_runtime .

# Build a runtime container based on the Dockerfile name
# Usage: just build-runtime sh (builds syft_sh_runtime from runtimes/sh.Dockerfile)
[group('utils')]
build-runtime runtime_name:
    #!/usr/bin/env bash
    echo "Building syft_{{runtime_name}}_runtime from runtimes/{{runtime_name}}.Dockerfile"
    docker build -t syft_{{runtime_name}}_runtime -f runtimes/{{runtime_name}}.Dockerfile .

# Build all runtime containers
[group('utils')]
build-all-runtimes:
    #!/usr/bin/env bash
    echo "Building all runtime containers..."
    for dockerfile in runtimes/*.Dockerfile; do
        runtime_name=$(basename "$dockerfile" .Dockerfile)
        if [ "$runtime_name" != "base" ]; then
            echo "Building syft_${runtime_name}_runtime..."
            just build-runtime "$runtime_name"
        fi
    done
    echo "All runtime containers built successfully!"

[group('test')]
test-integration:
    uv run --frozen --with "pytest" \
        pytest syft-rds/tests/integration/crud_test.py \
        syft-rds/tests/integration/dataset_test.py

[group('test')]
test-unit:
    uv run pytest --color=yes syft-rds/tests/unit/*_test.py

[group('test')]
test-e2e:
    #!/bin/sh
    echo "Using SyftBox from {{ _green }}'$(which syftbox)'{{ _nc }}"
    uv run pytest -sq --color=yes syft-rds/tests/e2e/*_test.py

[group('test')]
test:
    just test-unit
    just test-integration
    just test-e2e
