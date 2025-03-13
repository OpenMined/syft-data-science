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
_test_workers := env_var_or_default('TEST_WORKERS', 'auto')
_test_verbosity := env_var_or_default('TEST_VERBOSE', 'sq')

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

# remove all local files & directories
[group('utils')]
reset:
    #!/bin/sh
    cd syft-rds
    rm -rf ./.clients ./.server ./dist ./.e2e ./.logs


# ---------------------------------------------------------------------------------------------------------------------
[group('test')]
setup-test-env:
    #!/bin/sh
    cd syft-rds && uv sync --frozen --cache-dir=.uv-cache && . .venv/bin/activate

[group('test')]
test-unit: setup-test-env
    #!/bin/sh
    cd syft-rds && echo "{{ _cyan }}Running unit tests {{ _nc }}"
    uv run --with "pytest-xdist" pytest -{{ _test_verbosity }} --color=yes -n {{ _test_workers }} tests/unit/

[group('test')]
test-integration: setup-test-env
    #!/bin/sh
    cd syft-rds && echo "{{ _cyan }}Running integration tests {{ _nc }}"
    uv run --with "pytest-xdist" pytest -{{ _test_verbosity }} --color=yes -n {{ _test_workers }} tests/integration/

[group('test')]
test-e2e: setup-test-env
    #!/bin/sh
    cd syft-rds
    rm -rf .e2e/
    echo "{{ _cyan }}Running end-to-end tests {{ _nc }}"
    echo "Using SyftBox from {{ _green }}'$(which syftbox)'{{ _nc }}"
    uv run --with "pytest-xdist" pytest -{{ _test_verbosity }} --color=yes -n {{ _test_workers }} tests/e2e/

[group('test')]
test-notebooks: setup-test-env
    #!/bin/sh
    cd syft-rds
    echo "{{ _cyan }}Running notebook tests {{ _nc }}"

    uv run --with "nbmake" --with "pytest-xdist" pytest -{{ _test_verbosity }} --color=yes -n {{ _test_workers }} --nbmake ../notebooks/quickstart/full_flow.ipynb

[group('test')]
test: setup-test-env
    #!/bin/sh
    echo "{{ _cyan }}Running all tests in parallel{{ _nc }}"
    just test-unit &
    just test-integration &
    just test-e2e &
    just test-notebooks &
    wait
    echo "{{ _green }}All tests completed!{{ _nc }}"


# ---------------------------------------------------------------------------------------------------------------------

# Run a local syftbox client on any available port between 8080-9000
[group('syftbox')]
run-syftbox-server port="5001" gunicorn_args="":
    #!/bin/bash
    cd syft-rds
    set -eou pipefail

    export SYFTBOX_DATA_FOLDER=${SYFTBOX_DATA_FOLDER:-.server/data}
    uv run syftbox server migrate
    uv run gunicorn syftbox.server.server:app -k uvicorn.workers.UvicornWorker --bind 127.0.0.1:{{ port }} --reload {{ gunicorn_args }}

[group('syftbox')]
run-syftbox name port="auto" server="http://localhost:5001":
    #!/bin/bash
    cd syft-rds
    set -eou pipefail

    # generate a local email from name, but if it looks like an email, then use it as is
    EMAIL="{{ name }}@openmined.org"
    if [[ "{{ name }}" == *@*.* ]]; then EMAIL="{{ name }}"; fi

    # if port is auto, then generate a random port between 8000-8090, else use the provided port
    PORT="{{ port }}"
    if [[ "$PORT" == "auto" ]]; then PORT="0"; fi

    # Working directory for client is .clients/<email>
    DATA_DIR=.clients/$EMAIL
    mkdir -p $DATA_DIR

    echo -e "Email      : {{ _green }}$EMAIL{{ _nc }}"
    echo -e "Client     : {{ _cyan }}http://localhost:$PORT{{ _nc }}"
    echo -e "Server     : {{ _cyan }}{{ server }}{{ _nc }}"
    echo -e "Data Dir   : $DATA_DIR"

    uv run syftbox client --config=$DATA_DIR/config.json --data-dir=$DATA_DIR --email=$EMAIL --port=$PORT --server={{ server }} --no-open-dir

[group('syftbox')]
run-syftbox-do server="http://localhost:5001":
    #!/bin/bash
    just run-syftbox "data_owner@openmined.org" "auto" "{{ server }}"

[group('syftbox')]
run-syftbox-ds server="http://localhost:5001":
    #!/bin/bash
    just run-syftbox "data_scientist@openmined.org" "auto" "{{ server }}"

# ---------------------------------------------------------------------------------------------------------------------

[group('rds')]
run-rds-server syftbox_config="":
    #!/bin/bash
    if [ -z "{{ syftbox_config }}" ]; then
        cd syft-rds
        uv run syft-rds server
    else
        CONFIG_PATH=$(realpath "{{ syftbox_config }}")
        cd syft-rds
        uv run syft-rds server --syftbox-config "$CONFIG_PATH"
    fi

run-rds-do:
    #!/bin/bash
    just run-rds-server "syft-rds/.clients/data_owner@openmined.org/config.json"

[group('rds')]
install:
    #!/bin/bash
    cd syft-rds
    uv venv --allow-existing
    uv sync

[group('rds')]
run-rds-stack:
    #!/bin/bash
    just reset
    just install
    mkdir -p ./syft-rds/.logs

    echo "Launching syftbox-server..."
    just run-syftbox-server > ./syft-rds/.logs/syftbox-server.log 2>&1 &
    SERVER_PID=$!
    sleep 2

    echo "Launching syftbox-do..."
    just run-syftbox-do > ./syft-rds/.logs/syftbox-do.log 2>&1 &
    DO_PID=$!
    sleep 2

    echo "Launching syftbox-ds..."
    just run-syftbox-ds > ./syft-rds/.logs/syftbox-ds.log 2>&1 &
    DS_PID=$!
    sleep 2

    # Function to kill background processes
    function cleanup {
        kill $SERVER_PID $DO_PID $DS_PID
        exit 0
    }

    # Set up trap to catch Ctrl+C
    trap cleanup INT

    echo "Launching rds-do in foreground..."
    just run-rds-do | tee ./syft-rds/.logs/rds-do.log