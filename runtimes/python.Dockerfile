FROM python:3.12-slim

# Copy the common entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Create a restricted user with no home directory and no shell
RUN adduser --no-create-home --shell /sbin/nologin --disabled-password --gecos "" runtimeuser


# COPY dist/ /dist/
# RUN pip install /dist/*.whl


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