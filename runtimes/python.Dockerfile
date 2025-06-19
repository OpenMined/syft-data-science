FROM python:3.12-slim

# Embed the entrypoint.sh script directly (temp fix for entrypoint.sh not being copied)
# COPY entrypoint.sh /entrypoint.sh
# RUN chmod +x /entrypoint.sh
RUN echo '#!/bin/sh\n\
\n\
# This script serves as a common entrypoint for runtime containers\n\
# It handles timeout and passes arguments to the appropriate interpreter\n\
\n\
# Default values\n\
TIMEOUT=${TIMEOUT:-60}\n\
TIMEOUT_MESSAGE=${TIMEOUT_MESSAGE:-"Process timed out."}\n\
INTERPRETER=${INTERPRETER:-"sh"}\n\
\n\
# Set up timeout handling\n\
trap "echo \"$TIMEOUT_MESSAGE\"" TERM\n\
timeout -s TERM $TIMEOUT "$INTERPRETER" "$@" || {\n\
  test $? -eq 124 && echo "$TIMEOUT_MESSAGE" >&2 && exit 124\n\
}' > /entrypoint.sh && chmod +x /entrypoint.sh

# Create a restricted user with no home directory and no shell
RUN adduser --no-create-home --shell /sbin/nologin --disabled-password --gecos "" runtimeuser

# Create and set restrictive permissions on code directory
RUN mkdir -p /app/code && \
    chown runtimeuser:runtimeuser /app/code && \
    chmod 500 /app/code

# Create and set restrictive read and write permissions on output directory
RUN mkdir -p /app/output && \
    chown runtimeuser:runtimeuser /app/output && \
    chmod 755 /app/output

WORKDIR /app/code

# Set Python to not write bytecode and run in unbuffered mode
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

USER runtimeuser

# Set common environment variables
ENV TIMEOUT=60
ENV TIMEOUT_MESSAGE="Process timed out."
ENV INTERPRETER="python"

# Use the common entrypoint
ENTRYPOINT ["/entrypoint.sh"]
CMD []