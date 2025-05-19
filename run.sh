#!/usr/bin/env bash

# Load environment variables from .env if it exists
if [ -f ".env" ]; then
  set -o allexport
  source .env
  set +o allexport
fi

# Start the FastAPI webhook server
exec uvicorn webhook_server:app --host 0.0.0.0 --port 8000
