# First stage: Build the base image
FROM alpine:latest as base

# Create a restricted user with no home directory and no shell
RUN adduser -D -H -s /sbin/nologin -u 1000 runtimeuser

# Copy the common entrypoint script
COPY runtimes/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Create and set restrictive permissions on code directory
RUN mkdir /code && \
    chown runtimeuser:runtimeuser /code && \
    chmod 500 /code

# Second stage: Final image
FROM alpine:latest

# Copy from base stage
COPY --from=base /entrypoint.sh /entrypoint.sh
COPY --from=base /etc/passwd /etc/passwd
COPY --from=base /etc/group /etc/group

# Create and set restrictive permissions on code directory
RUN mkdir /code && \
    chmod 500 /code && \
    chown runtimeuser:runtimeuser /code

WORKDIR /code

USER runtimeuser

# Set common environment variables
ENV TIMEOUT=60
ENV TIMEOUT_MESSAGE="Process timed out."
ENV INTERPRETER="sh"

# Use the common entrypoint
ENTRYPOINT ["/entrypoint.sh"]
CMD [] 