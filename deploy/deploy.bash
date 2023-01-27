#!/usr/bin/env bash

# Deployment script - intended to run on Drones servers

# Colors
C_RESET='\033[0m'
C_RED='\033[1;31m'
C_GREEN='\033[1;32m'
C_YELLOW='\033[1;33m'

# Logs
PREFIX_INFO="${C_GREEN}[INFO]${C_RESET} [$(date +%d-%m\ %T)]"
PREFIX_WARN="${C_YELLOW}[WARN]${C_RESET} [$(date +%d-%m\ %T)]"
PREFIX_CRIT="${C_RED}[CRIT]${C_RESET} [$(date +%d-%m\ %T)]"

# Main
APP_DIR="${APP_DIR:-/home/ubuntu/drones}"
AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
PYTHON_ENV_DIR="${PYTHON_ENV_DIR:-/home/ubuntu/drones-env}"
PYTHON="${PYTHON_ENV_DIR}/bin/python"
PIP="${PYTHON_ENV_DIR}/bin/pip"
SCRIPT_DIR="$(realpath $(dirname $0))"
SECRETS_DIR="${SECRETS_DIR:-/home/ubuntu/drones-secrets}"
PARAMETERS_ENV_PATH="${SECRETS_DIR}/app.env"

DRONES_SERVICE_FILE="drones.service"
# Drones statistics generator
DRONES_STATISTICS_SERVICE_FILE="drones-statistics.service"
DRONES_STATISTICS_TIMER_FILE="drones-statistics.timer"
# Drones Humbug report loader
DRONES_HUMBUG_REPORT_LOADER_FILE="drones-humbug-reports.service"
# Drones journal rules
DRONES_RULE_UNLOCK_SERVICE_FILE="drones-rule-unlock.service"
DRONES_RULE_UNLOCK_TIMER_FILE="drones-rule-unlock.timer"

set -eu

echo
echo
echo -e "${PREFIX_INFO} Upgrading Python pip and setuptools"
"${PIP}" install --upgrade pip setuptools

echo
echo
echo "Updating Python dependencies"
"${PIP}" install --exists-action i -r "${APP_DIR}/requirements.txt"

echo
echo
echo -e "${PREFIX_INFO} Install checkenv"
HOME=/home/ubuntu /usr/local/go/bin/go install github.com/bugout-dev/checkenv@latest

echo
echo
echo -e "${PREFIX_INFO} Retrieving deployment parameters"
mkdir -p "${SECRETS_DIR}"
AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION}" /home/ubuntu/go/bin/checkenv show aws_ssm+drones:true > "${PARAMETERS_ENV_PATH}"
chmod 0640 "${PARAMETERS_ENV_PATH}"

echo
echo
echo -e "${PREFIX_INFO} Replacing existing Drones service definition with ${DRONES_SERVICE_FILE}"
chmod 644 "${SCRIPT_DIR}/${DRONES_SERVICE_FILE}"
cp "${SCRIPT_DIR}/${DRONES_SERVICE_FILE}" "/home/ubuntu/.config/systemd/user/${DRONES_SERVICE_FILE}"
XDG_RUNTIME_DIR="/run/user/1000" systemctl --user daemon-reload
XDG_RUNTIME_DIR="/run/user/1000" systemctl --user restart --no-block "${DRONES_SERVICE_FILE}"

echo
echo
echo -e "${PREFIX_INFO} Replacing existing Drones Statistics generation service and timer with: ${DRONES_STATISTICS_SERVICE_FILE}, ${DRONES_STATISTICS_TIMER_FILE}"
chmod 644 "${SCRIPT_DIR}/${DRONES_STATISTICS_SERVICE_FILE}" "${SCRIPT_DIR}/${DRONES_STATISTICS_TIMER_FILE}"
cp "${SCRIPT_DIR}/${DRONES_STATISTICS_SERVICE_FILE}" "/home/ubuntu/.config/systemd/user/${DRONES_STATISTICS_SERVICE_FILE}"
cp "${SCRIPT_DIR}/${DRONES_STATISTICS_TIMER_FILE}" "/home/ubuntu/.config/systemd/user/${DRONES_STATISTICS_TIMER_FILE}"
XDG_RUNTIME_DIR="/run/user/1000" systemctl --user daemon-reload
XDG_RUNTIME_DIR="/run/user/1000" systemctl --user restart --no-block "${DRONES_STATISTICS_TIMER_FILE}"

echo
echo
echo -e "${PREFIX_INFO} Replacing existing humbug report loader service definition with ${DRONES_HUMBUG_REPORT_LOADER_FILE}"
chmod 644 "${SCRIPT_DIR}/${DRONES_HUMBUG_REPORT_LOADER_FILE}"
cp "${SCRIPT_DIR}/${DRONES_HUMBUG_REPORT_LOADER_FILE}" "/home/ubuntu/.config/systemd/user/${DRONES_HUMBUG_REPORT_LOADER_FILE}"
XDG_RUNTIME_DIR="/run/user/1000" systemctl --user daemon-reload
XDG_RUNTIME_DIR="/run/user/1000" systemctl --user restart --no-block "${DRONES_HUMBUG_REPORT_LOADER_FILE}"

echo
echo
echo -e "${PREFIX_INFO} Replacing existing Drones unlock rule service and timer with: ${DRONES_RULE_UNLOCK_SERVICE_FILE}, ${DRONES_RULE_UNLOCK_TIMER_FILE}"
chmod 644 "${SCRIPT_DIR}/${DRONES_RULE_UNLOCK_SERVICE_FILE}" "${SCRIPT_DIR}/${DRONES_RULE_UNLOCK_TIMER_FILE}"
cp "${SCRIPT_DIR}/${DRONES_RULE_UNLOCK_SERVICE_FILE}" "/home/ubuntu/.config/systemd/user/${DRONES_RULE_UNLOCK_SERVICE_FILE}"
cp "${SCRIPT_DIR}/${DRONES_RULE_UNLOCK_TIMER_FILE}" "/home/ubuntu/.config/systemd/user/${DRONES_RULE_UNLOCK_TIMER_FILE}"
XDG_RUNTIME_DIR="/run/user/1000" systemctl --user daemon-reload
XDG_RUNTIME_DIR="/run/user/1000" systemctl --user restart --no-block "${DRONES_RULE_UNLOCK_TIMER_FILE}"
