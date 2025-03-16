#!/bin/bash
set -e

# Function to check if RDS server is running
check_server_running() {
  # Look specifically for the RDS server process
  if pgrep -f "syft-rds server" > /dev/null; then
    return 0  # Server is running
  else
    return 1  # Server is not running
  fi
}

# Main logic
if check_server_running; then
  echo "RDS server is already running."
else
  echo "Starting RDS server..."

  # Use the run-rds-server command to start just the RDS server
  # This does not start a SyftBox client
  just run-rds-server &

  # Wait for server to start
  echo "Waiting for server to start..."
  for i in {1..10}; do
    sleep 1
    if check_server_running; then
      echo "RDS server started successfully."
      break
    fi
    if [ $i -eq 10 ]; then
      echo "Failed to start RDS server within timeout period."
      exit 1
    fi
  done
fi

echo "RDS server status check completed."
