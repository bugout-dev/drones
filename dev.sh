#!/usr/bin/env bash

# Expects access to Python environment with the requirements for this project installed.
set -e

DRONES_HOST="${DRONES_HOST:-0.0.0.0}"
DRONES_PORT="${DRONES_PORT:-7476}"

uvicorn --port "$DRONES_PORT" --host "$DRONES_HOST" drones.api:app --workers 2 $@
