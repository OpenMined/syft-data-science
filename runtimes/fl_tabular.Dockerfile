# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Install git and other required packages
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    libffi-dev \
    git \
    tmux \
    && rm -rf /var/lib/apt/lists/*

RUN pip install uv

WORKDIR /app

# TODO: remove this copy and install syft-flwr from pip
COPY ./syft-flwr/ ./syft-flwr/
# Install dependencies (will be cached if dependencies don't change)
RUN uv venv && \
    . .venv/bin/activate && \
    cd syft-flwr && \
    uv sync --active && \
    uv pip install "scikit-learn==1.6.1" "torch==2.5.1"

# Define environment variables needed by main.py
# You might need to adjust these paths or mount volumes when running the container
ENV DATA_DIR=/app/data
ENV OUTPUT_DIR=/app/output
ENV SYFTBOX_CLIENT_CONFIG_PATH=/app/config.json

# Create the data and output directories
RUN mkdir -p $OUTPUT_DIR $DATA_DIR

# Run main.py when the container launches
# Set the working directory to the fl-tabular project folder
# WORKDIR /app/fl-tabular
# ENTRYPOINT ["./.venv/bin/python", "fl-tabular/main.py"]
# CMD ["--active"]
RUN echo '#!/bin/sh\n\
tmux new-session -d -s rds-server "uv run syft-rds server"\n\
sleep 1\n\
exec ./.venv/bin/python /app/code/main.py --active\n\
' > /app/start.sh && chmod +x /app/start.sh

ENTRYPOINT ["/app/start.sh"]