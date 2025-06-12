FROM python:3.12-slim

# Install git for git-based dependencies, as some deps might be from git.
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# Install uv, the fast python package installer
RUN pip install uv

# Copy project files needed for installation
COPY pyproject.toml poetry.lock* README.md /app/
COPY src /app/src

WORKDIR /app

# Install the project and all its dependencies using uv.
# We use --system to install into the global python site-packages, not a venv.
# -e installs in editable mode.
# --all-extras installs optional dependencies for test, dev, etc.
RUN uv pip install --system -e . --all-extras

# Now, setup the runtime environment for the job execution
# This part is similar to the production Dockerfile

# Copy the common entrypoint script
COPY runtimes/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Create a restricted user with no home directory and no shell
RUN adduser --no-create-home --shell /sbin/nologin --disabled-password --gecos "" runtimeuser

# Create and set restrictive permissions on code directory for user code
RUN mkdir /code && \
    chown runtimeuser:runtimeuser /code && \
    chmod 500 /code

# Create and set restrictive read and write permissions on output directory
RUN mkdir /output && \
    chown runtimeuser:runtimeuser /output && \
    chmod -R 777 /output

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