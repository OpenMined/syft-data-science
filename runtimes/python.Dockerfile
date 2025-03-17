FROM python:3.12-slim

# Copy the common entrypoint script
COPY runtimes/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Create a restricted user with no home directory and no shell
RUN adduser --no-create-home --shell /sbin/nologin --disabled-password --gecos "" runtimeuser


# COPY dist/ /dist/
# RUN pip install /dist/*.whl

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