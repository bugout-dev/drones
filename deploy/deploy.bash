#!/usr/bin/env bash

# Deployment script - intended to run on Drones servers

# Main
APP_DIR="${APP_DIR:-/home/ubuntu/drones}"
AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
PYTHON_ENV_DIR="${PYTHON_ENV_DIR:-/home/ubuntu/drones-env}"
PYTHON="${PYTHON_ENV_DIR}/bin/python"
PIP="${PYTHON_ENV_DIR}/bin/pip"
SCRIPT_DIR="$(realpath $(dirname $0))"
PARAMETERS_SCRIPT="${SCRIPT_DIR}/parameters.py"
SECRETS_DIR="${SECRETS_DIR:-/home/ubuntu/drones-secrets}"
PARAMETERS_ENV_PATH="${SECRETS_DIR}/app.env"
SERVICE_FILE="${SCRIPT_DIR}/drones.service"

# Drones statistics generator
DRONES_STATISTICS_SERVICE_FILE="${SCRIPT_DIR}/dronesstatistics.service"
DRONES_STATISTICS_TIMER_FILE="${SCRIPT_DIR}/dronesstatistics.timer"

set -eu

echo
echo
echo "Updating Python dependencies"
sudo -u ubuntu "${PIP}" install -r "${APP_DIR}/requirements.txt"

echo
echo
echo "Retrieving deployment parameters"
mkdir -p "${SECRETS_DIR}"
AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION}" "${PYTHON}" "${PARAMETERS_SCRIPT}" extract -o "${PARAMETERS_ENV_PATH}"

echo
echo
echo "Replacing existing Drones service definition with ${SERVICE_FILE}"
chmod 644 "${SERVICE_FILE}"
cp "${SERVICE_FILE}" /etc/systemd/system/drones.service
systemctl daemon-reload
systemctl restart drones.service
systemctl status drones.service

echo
echo
echo "Replacing existing Drones Statistics generation service and timer with: ${DRONES_STATISTICS_SERVICE_FILE}, ${DRONES_STATISTICS_TIMER_FILE}"
chmod 644 "${DRONES_STATISTICS_SERVICE_FILE}" "${DRONES_STATISTICS_TIMER_FILE}"
cp "${DRONES_STATISTICS_SERVICE_FILE}" /etc/systemd/system/dronesstatistics.service
cp "${DRONES_STATISTICS_TIMER_FILE}" /etc/systemd/system/dronesstatistics.timer
systemctl daemon-reload
systemctl restart dronesstatistics.timer