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

[group('test')]
run-rds-integration-tests:
    uv run --frozen --with "pytest" \
        pytest syft-rds/tests/integration/crud_test.py \
        syft-rds/tests/integration/dataset_test.py

[group('test')]
run-rds-unit-tests:
    uv run pytest syft-rds/tests/unit/*_test.py

[group('test')]
run-tests:
    just run-rds-unit-tests
    just run-rds-integration-tests