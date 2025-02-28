# First stage: Build the base with entrypoint script
FROM alpine:latest as base

# Copy the common entrypoint script
COPY runtimes/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Second stage: Final Python image
FROM python:3.12-slim

# Copy entrypoint script from base
COPY --from=base /entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Create a restricted user with no home directory and no shell
RUN adduser --no-create-home --shell /sbin/nologin --disabled-password --uid 1000 runtimeuser

# COPY dist/ /dist/
# RUN pip install /dist/*.whl

# Create and set restrictive permissions on code directory
RUN mkdir /code && \
    chown runtimeuser:runtimeuser /code && \
    chmod 500 /code

WORKDIR /code

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