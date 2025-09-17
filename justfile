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
run-jupyter:
    #!/bin/bash
    uv venv
    uv sync
    uv run jupyter-lab

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
clean:
    #!/bin/sh
    rm -rf ./.clients ./.server ./dist ./.e2e ./.logs **/__pycache__ ./.pytest_cache/

# ---------------------------------------------------------------------------------------------------------------------
[group('test')]
setup-test-env:
    #!/bin/sh
    if [ ! -d ".venv" ]; then
        uv venv
    fi
    uv sync --cache-dir=.uv-cache

[group('test')]
test-unit: setup-test-env
    #!/bin/sh
    echo "{{ _cyan }}Running unit tests {{ _nc }}"
    uv run --with "pytest-xdist" pytest -{{ _test_verbosity }} --color=yes -n {{ _test_workers }} tests/unit/

[group('test')]
test-integration: setup-test-env
    #!/bin/sh
    echo "{{ _cyan }}Running integration tests {{ _nc }}"
    uv run --with "pytest-xdist" pytest -{{ _test_verbosity }} --color=yes -n {{ _test_workers }} tests/integration/

[group('test')]
test-e2e: setup-test-env
    #!/bin/sh
    rm -rf .e2e/
    echo "{{ _cyan }}Running end-to-end tests {{ _nc }}"
    echo "Using SyftBox from {{ _green }}'$(which syftbox)'{{ _nc }}"
    uv run --with "pytest-xdist" pytest -{{ _test_verbosity }} --color=yes -n {{ _test_workers }} tests/e2e/

[group('test')]
test-notebooks: setup-test-env
    #!/bin/sh
    echo "{{ _cyan }}Running notebook tests {{ _nc }}"
    uv run --with "nbmake" \
        --with "pytest-xdist" pytest -{{ _test_verbosity }} \
        --color=yes -n {{ _test_workers }} \
        --nbmake notebooks/quickstart/full_flow.ipynb

[group('test')]
test: setup-test-env
    #!/bin/sh
    echo "{{ _cyan }}Running all tests in parallel{{ _nc }}"
    just test-unit &
    just test-integration &
    # just test-e2e &
    just test-notebooks &
    wait
    just clean

# ---------------------------------------------------------------------------------------------------------------------

# Run a local syftbox client on any available port between 8080-9000
[group('syftbox')]
run-syftbox-server port="5001" gunicorn_args="":
    #!/bin/bash
    set -eou pipefail
    export SYFTBOX_DATA_FOLDER=${SYFTBOX_DATA_FOLDER:-.server/data}
    uv run syftbox server migrate
    uv run gunicorn syftbox.server.server:app -k uvicorn.workers.UvicornWorker --bind 127.0.0.1:{{ port }} --reload {{ gunicorn_args }}

[group('syftbox')]
run-syftbox name port="auto" server="http://localhost:5001":
    #!/bin/bash
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

    export SYFTBOX_DISABLE_ICONS=1
    uv run syftbox client --config=$DATA_DIR/config.json --data-dir=$DATA_DIR --email=$EMAIL --port=$PORT --server={{ server }} --no-open-dir

# ---------------------------------------------------------------------------------------------------------------------

[group('rds')]
run-rds-server syftbox_config="":
    #!/bin/bash
    if [ -z "{{ syftbox_config }}" ]; then
        uv run syft-rds server
    else
        CONFIG_PATH=$(realpath "{{ syftbox_config }}")
        uv run syft-rds server --syftbox-config "$CONFIG_PATH"
    fi

[group('rds')]
install:
    #!/bin/bash
    uv venv
    uv sync


# Run both a syftbox client and an RDS server
[group('rds')]
run-syftbox-rds name server="http://localhost:5001":
    #!/bin/bash
    set -eou pipefail
    mkdir -p ./syft_rds/.logs

    echo "Starting SyftBox client for {{ name }}..."
    just run-syftbox "{{ name }}" "auto" {{ server }} > ./syft_rds/.logs/syftbox-{{ name }}.log 2>&1 &
    CLIENT_PID=$!

    # Give client time to start and create config
    sleep 2

    # Set trap to directly kill the client process when this script exits
    trap "kill $CLIENT_PID; exit 0" INT

    # Get config path for this client
    CONFIG_PATH="$(pwd)/syft_rds/.clients/{{ name }}/config.json"

    echo "Starting RDS server with config from $CONFIG_PATH..."
    just run-rds-server "$CONFIG_PATH"

# Run the full RDS stack with datasites
[group('rds')]
run-rds-stack client_names="data_owner@openmined.org data_scientist@openmined.org":
    #!/bin/bash
    set -eou pipefail

    # Setup environment
    just reset
    just install
    mkdir -p ./syft_rds/.logs

    echo "Setting up stack with clients: {{ client_names }}"

    # Start SyftBox server
    echo "Launching SyftBox server..."
    just run-syftbox-server > ./syft_rds/.logs/syftbox-server.log 2>&1 &
    SERVER_PID=$!
    sleep 2

    # Track all PIDs for cleanup
    declare -a ALL_PIDS=($SERVER_PID)

    # Split the space-separated list into an array
    clients=( {{client_names}} )

    # Launch syftbox clients and RDS servers
    for client in "${clients[@]}"; do
        echo "Starting $client..."

        # Run in background and capture its PID, logging output to files
        (just run-syftbox-rds "$client" "http://localhost:5001") > ./syft_rds/.logs/rds-$client.log 2>&1 &
        STACK_PID=$!
        ALL_PIDS+=($STACK_PID)
    done

    sleep 2

    # Function to kill all processes
    function cleanup {
        echo "Shutting down all processes..."
        for pid in "${ALL_PIDS[@]}"; do
            kill $pid 2>/dev/null || true
        done
        exit 0
    }

    # Set trap to catch Ctrl+C
    trap cleanup INT

    echo "All services started successfully!"
    echo "Logs available in: $(pwd)/syft_rds/.logs/"
    echo "Press Ctrl+C to shut down"

    # Wait forever (until Ctrl+C)
    tail -f /dev/null

# ---------------------------------------------------------------------------------------------------------------------

# Bump version in pyproject.toml and __init__.py
# Usage: just bump-version patch/minor/major
[group('build')]
bump-version version_type="patch":
    #!/bin/bash
    set -eou pipefail

    # Check if version_type is valid
    if [[ "{{ version_type }}" != "patch" && "{{ version_type }}" != "minor" && "{{ version_type }}" != "major" ]]; then
        echo "{{ _red }}Error: Invalid version type '{{ version_type }}'. Use: patch, minor, or major{{ _nc }}"
        exit 1
    fi

    # Get current version from pyproject.toml
    CURRENT_VERSION=$(grep '^version = ' pyproject.toml | sed 's/version = "\(.*\)"/\1/')

    # Parse version components
    IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT_VERSION"

    # Bump version based on type
    if [[ "{{ version_type }}" == "major" ]]; then
        NEW_MAJOR=$((MAJOR + 1))
        NEW_VERSION="${NEW_MAJOR}.0.0"
    elif [[ "{{ version_type }}" == "minor" ]]; then
        NEW_MINOR=$((MINOR + 1))
        NEW_VERSION="${MAJOR}.${NEW_MINOR}.0"
    else  # patch
        NEW_PATCH=$((PATCH + 1))
        NEW_VERSION="${MAJOR}.${MINOR}.${NEW_PATCH}"
    fi

    # Update version in pyproject.toml
    sed -i.bak "s/^version = \".*\"/version = \"$NEW_VERSION\"/" pyproject.toml && rm pyproject.toml.bak

    # Update version in __init__.py
    sed -i.bak "s/^__version__ = \".*\"/__version__ = \"$NEW_VERSION\"/" src/syft_rds/__init__.py && rm src/syft_rds/__init__.py.bak

    echo ""
    echo -e "{{ _green }}✓ Version bumped: $CURRENT_VERSION → $NEW_VERSION{{ _nc }}"
    echo ""
    echo -e "{{ _cyan }}Updated files:{{ _nc }}"
    echo "  • pyproject.toml"
    echo "  • src/syft_rds/__init__.py"
    echo ""
    echo -e "{{ _cyan }}Next steps:{{ _nc }}"
    echo "  0. just test"
    echo "  1. git add pyproject.toml src/syft_rds/__init__.py"
    echo "  2. git commit -m \"Bump version to $NEW_VERSION\""
    echo "  3. git tag v$NEW_VERSION"
    echo "  4. git push && git push --tags"
    echo "  5. just build"

# Build syft rds wheel
[group('build')]
build:
    @echo "{{ _cyan }}Building syft-rds wheel...{{ _nc }}"
    rm -rf dist/
    uv build
    @echo "{{ _green }}Build complete!{{ _nc }}"
    @echo "{{ _cyan }}To inspect the build:{{ _nc }}"
    @echo "{{ _cyan }}1. Go to the build directory and unzip the .tar.gz file to inspect the contents{{ _nc }}"
    @echo "{{ _cyan }}2. Inspect the .whl file with: uvx wheel unpack <path_to_whl_file>{{ _nc }}"
    @echo "{{ _cyan }}3. To upload to pypi, run: uvx twine upload ./dist/*{{ _nc }}"
# ---------------------------------------------------------------------------------------------------------------------

