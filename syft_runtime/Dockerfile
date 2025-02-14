FROM python:3.12-slim

# Create a restricted user with no home directory and no shell
RUN adduser --no-create-home --shell /sbin/nologin --disabled-password --uid 1000 pythonuser

COPY dist/ /dist/
RUN pip install /dist/*.whl

# Create and set restrictive permissions on code directory
RUN mkdir /code && \
    chown pythonuser:pythonuser /code && \
    chmod 500 /code

WORKDIR /code

# Set Python to not write bytecode and run in unbuffered mode
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

USER pythonuser

ENV TIMEOUT=60

ENTRYPOINT ["sh", "-c", "timeout ${TIMEOUT}s python main.py || echo \"Process timed out after ${TIMEOUT} seconds\""]