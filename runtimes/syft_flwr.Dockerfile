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

RUN uv venv && \
    . .venv/bin/activate && \
    uv pip install \
    "flwr[simulation]==1.17.0" "flwr-datasets>=0.5.0" \
    "scikit-learn==1.6.1" "torch==2.7.0" \
    "syft_flwr==0.1.3" "syft_rds==0.1.3" \
    "syft_core" \
    imblearn pandas loguru \
    seaborn matplotlib

ENV DATA_DIR=/app/data
ENV OUTPUT_DIR=/app/output
ENV SYFTBOX_CLIENT_CONFIG_PATH=/app/config.json

RUN mkdir -p $OUTPUT_DIR $DATA_DIR

RUN echo '#!/bin/sh\n\
tmux new-session -d -s rds-server "uv run syft-rds server"\n\
sleep 1\n\
exec ./.venv/bin/python /app/code/main.py --active\n\
' > /app/start.sh && chmod +x /app/start.sh

ENTRYPOINT ["/app/start.sh"]