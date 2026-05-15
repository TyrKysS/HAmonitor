#!/usr/bin/with-contenv bashio

bashio::log.info "Starting HA Monitor..."

DOMAINS=$(bashio::config 'domains')
OUTPUT_DIR=$(bashio::config 'output_dir')
LOG_STDOUT=$(bashio::config 'log_to_stdout')

export HA_TOKEN="${SUPERVISOR_TOKEN}"
export MONITOR_DOMAINS="${DOMAINS}"
export MONITOR_OUTPUT_DIR="${OUTPUT_DIR}"
export MONITOR_LOG_STDOUT="${LOG_STDOUT}"

python3 /app/monitor.py
