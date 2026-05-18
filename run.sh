#!/usr/bin/with-contenv bashio

bashio::log.info "Starting HA Monitor..."

DOMAINS=$(bashio::config 'domains')
OUTPUT_DIR=$(bashio::config 'output_dir')
LOG_STDOUT=$(bashio::config 'log_to_stdout')
SCORE_MIN=$(bashio::config 'score_min_threshold')
OLLAMA_ENABLED=$(bashio::config 'ollama_enabled')
OLLAMA_URL=$(bashio::config 'ollama_url')
OLLAMA_MODEL=$(bashio::config 'ollama_model')
OLLAMA_SCORE_THRESHOLD=$(bashio::config 'ollama_score_threshold')
OLLAMA_LANGUAGE=$(bashio::config 'ollama_language')

export HA_TOKEN="${SUPERVISOR_TOKEN}"
export MONITOR_DOMAINS="${DOMAINS}"
export MONITOR_OUTPUT_DIR="${OUTPUT_DIR}"
export MONITOR_LOG_STDOUT="${LOG_STDOUT}"
export MONITOR_SCORE_MIN_THRESHOLD="${SCORE_MIN}"
export MONITOR_OLLAMA_ENABLED="${OLLAMA_ENABLED}"
export MONITOR_OLLAMA_URL="${OLLAMA_URL}"
export MONITOR_OLLAMA_MODEL="${OLLAMA_MODEL}"
export MONITOR_OLLAMA_SCORE_THRESHOLD="${OLLAMA_SCORE_THRESHOLD}"
export MONITOR_OLLAMA_LANGUAGE="${OLLAMA_LANGUAGE}"

AUTOMATIONS=$(bashio::config 'automations')
export MONITOR_AUTOMATIONS="${AUTOMATIONS}"

LLM_ACTIONS_ENABLED=$(bashio::config 'llm_actions_enabled')
LLM_ACTIONS_DOMAINS=$(bashio::config 'llm_actions_domains')
export MONITOR_LLM_ACTIONS_ENABLED="${LLM_ACTIONS_ENABLED}"
export MONITOR_LLM_ACTIONS_DOMAINS="${LLM_ACTIONS_DOMAINS}"

python3 /app/monitor.py
