#!/bin/sh

# This script serves as a common entrypoint for runtime containers
# It handles timeout and passes arguments to the appropriate interpreter

# Default values
TIMEOUT=${TIMEOUT:-60}
TIMEOUT_MESSAGE=${TIMEOUT_MESSAGE:-"Process timed out."}
INTERPRETER=${INTERPRETER:-"sh"}

# Set up timeout handling
trap 'echo "$TIMEOUT_MESSAGE"' TERM
timeout -s TERM $TIMEOUT $INTERPRETER "$@" || \
{ test $? -eq 124 && echo "$TIMEOUT_MESSAGE" >&2 && exit 124; }