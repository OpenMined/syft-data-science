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

# ---------------------------------------------------------------------------------------------------------------------
# Build a runtime container based on the Dockerfile name
# Usage: just build-runtime sh (builds syft_sh_runtime from runtimes/sh.Dockerfile)
[group('utils')]
build-runtime runtime_name:
    #!/usr/bin/env bash
    echo "{{ _cyan }}Building syft_{{runtime_name}}_runtime from runtimes/{{runtime_name}}.Dockerfile{{ _nc }}"
    docker build -t syft_{{runtime_name}}_runtime -f runtimes/{{runtime_name}}.Dockerfile .

# Build all runtime containers
[group('utils')]
build-all-runtimes:
    #!/usr/bin/env bash
    echo "{{ _cyan }}Building all runtime containers...{{ _nc }}"
    for dockerfile in runtimes/*.Dockerfile; do
        runtime_name=$(basename "$dockerfile" .Dockerfile)
        if [ "$runtime_name" != "base" ]; then
            echo "Building syft_${runtime_name}_runtime..."
            just build-runtime "$runtime_name"
        fi
    done
    echo "All runtime containers built successfully!"

# ---------------------------------------------------------------------------------------------------------------------
[group('test')]
setup-test-env:
    #!/bin/sh
    cd syft-rds && uv sync --frozen && . .venv/bin/activate

[group('test')]
test-unit: setup-test-env
    #!/bin/sh
    cd syft-rds && echo "{{ _cyan }}Running unit tests {{ _nc }}"
    uv run --with "pytest-xdist" pytest -sq --color=yes -n 4 tests/unit/*_test.py

[group('test')]
test-integration: setup-test-env
    #!/bin/sh
    cd syft-rds && echo "{{ _cyan }}Running integration tests {{ _nc }}"
    uv run --with "pytest-xdist" pytest -sq --color=yes -n 4 tests/integration/

[group('test')]
test-e2e: setup-test-env
    #!/bin/sh
    rm -rf .e2e/
    cd syft-rds
    echo "{{ _cyan }}Running end-to-end tests {{ _nc }}"
    echo "Using SyftBox from {{ _green }}'$(which syftbox)'{{ _nc }}"
    uv run --with "pytest-xdist" pytest -sq --color=yes -n 4 tests/e2e/*_test.py

[group('test')]
test-notebooks: setup-test-env
    #!/bin/sh
    cd syft-rds
    echo "{{ _cyan }}Running notebook tests {{ _nc }}"

    uv run --with "nbmake" --with "pytest-xdist" pytest -sq --color=yes -n 4 --nbmake ../notebooks/quickstart/full_flow.ipynb

[group('test')]
test:
    just test-unit
    just test-integration
    just test-e2e
    just test-notebooks
