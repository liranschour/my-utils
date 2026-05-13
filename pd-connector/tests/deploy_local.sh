#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the vLLM project
#
# Deploy a PD-disaggregated vLLM environment locally (no OC/k8s required).
# Both prefiller and decoder run on the local machine.
#
# Usage:
#   bash deploy_local.sh --config configs/local_pd.env
#   LOG_LEVEL=DEBUG bash deploy_local.sh --config configs/local_pd.env

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

# ---------------------------------------------------------------------------
# Parse --config
# ---------------------------------------------------------------------------
CONFIG_FILE=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --config) CONFIG_FILE="$2"; shift 2 ;;
        *) echo "Unknown argument: $1" >&2; exit 1 ;;
    esac
done

if [[ -z "$CONFIG_FILE" ]]; then
    echo "Usage: bash deploy_local.sh --config <config_file>" >&2
    echo "Example: bash deploy_local.sh --config configs/local_pd.env" >&2
    exit 1
fi

# Source config — env vars already set in the caller take precedence.
while IFS= read -r line || [[ -n "$line" ]]; do
    [[ "$line" =~ ^[[:space:]]*# ]] && continue
    [[ -z "${line//[[:space:]]/}" ]] && continue
    if [[ "$line" =~ ^([A-Za-z_][A-Za-z0-9_]*)=(.*)$ ]]; then
        _key="${BASH_REMATCH[1]}"
        _val="${BASH_REMATCH[2]}"
        [[ -z "${!_key+x}" ]] && export "${_key}=${_val}"
    fi
done < "$CONFIG_FILE"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
CONNECTOR="${CONNECTOR:-pd_connector}"
PREFILLER_GPUS="${PREFILLER_GPUS:-0}"
DECODER_GPUS="${DECODER_GPUS:-0}"

MODEL="${MODEL:-Qwen/Qwen3-0.6B}"
GPU_MEM_UTIL="${GPU_MEM_UTIL:-0.45}"
MAX_MODEL_LEN="${MAX_MODEL_LEN:-512}"
BLOCK_SIZE="${BLOCK_SIZE:-128}"
CPU_BYTES="${CPU_BYTES:-209715200}"
DECODER_FIRST="${DECODER_FIRST:-false}"

if [[ -z "${VLLM_BIN:-}" && -x "${REPO_ROOT}/.venv/bin/vllm" ]]; then
    VLLM_BIN="${REPO_ROOT}/.venv/bin/vllm"
else
    VLLM_BIN="${VLLM_BIN:-vllm}"
fi
if [[ -z "${PYTHON_BIN:-}" && -x "${REPO_ROOT}/.venv/bin/python" ]]; then
    PYTHON_BIN="${REPO_ROOT}/.venv/bin/python"
else
    PYTHON_BIN="${PYTHON_BIN:-python}"
fi

# Ensure the directory containing VLLM_BIN is on PATH so that child processes
# (e.g. FlashInfer JIT calling ninja) can find sibling binaries in the venv.
_VLLM_DIR="$(cd "$(dirname "$(command -v "${VLLM_BIN}" || echo "${VLLM_BIN}")")" 2>/dev/null && pwd)"
if [[ -n "$_VLLM_DIR" && ":$PATH:" != *":$_VLLM_DIR:"* ]]; then
    export PATH="${_VLLM_DIR}:${PATH}"
fi

LOG_LEVEL="${LOG_LEVEL:-INFO}"
HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-600}"

PREFILLER_HTTP_PORT="${PREFILLER_HTTP_PORT:-8100}"
DECODER_HTTP_PORT="${DECODER_HTTP_PORT:-8200}"
PROXY_PORT="${PROXY_PORT:-8192}"
PREFILLER_PD_PORT="${PREFILLER_PD_PORT:-7777}"
DECODER_PD_PORT="${DECODER_PD_PORT:-7778}"
NIXL_SIDE_CHANNEL_PORT_PREFILLER="${NIXL_SIDE_CHANNEL_PORT_PREFILLER:-5559}"
NIXL_SIDE_CHANNEL_PORT_DECODER="${NIXL_SIDE_CHANNEL_PORT_DECODER:-5659}"

PREFILLER_LOG="${PREFILLER_LOG:-/tmp/prefiller.log}"
DECODER_LOG="${DECODER_LOG:-/tmp/decoder.log}"
PROXY_LOG="${PROXY_LOG:-/tmp/proxy.log}"
STATE_FILE="${STATE_FILE:-/tmp/deploy_state.env}"

echo "=== Local Deploy: ${CONNECTOR} ==="
echo "  Prefiller: gpu=${PREFILLER_GPUS} http=:${PREFILLER_HTTP_PORT}"
echo "  Decoder:   gpu=${DECODER_GPUS} http=:${DECODER_HTTP_PORT}"
echo "  Proxy:     http=:${PROXY_PORT}"
echo "  Model:     ${MODEL}  gpu_mem=${GPU_MEM_UTIL}  max_len=${MAX_MODEL_LEN}"
echo ""

# ---------------------------------------------------------------------------
# Cleanup: kill child processes on exit
# ---------------------------------------------------------------------------
PIDS=()
cleanup() {
    echo ""
    echo "=== Cleaning up ==="
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    wait 2>/dev/null || true
    echo "Done."
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# Stop existing instances
# ---------------------------------------------------------------------------
echo "=== Stopping existing instances ==="
pkill -9 -f "vllm serve.*${PREFILLER_HTTP_PORT}" 2>/dev/null || true
pkill -9 -f "vllm serve.*${DECODER_HTTP_PORT}"   2>/dev/null || true
pkill -9 -f "pd_connector_proxy.py"              2>/dev/null || true
pkill -9 -f "toy_proxy_server.py"                2>/dev/null || true
sleep 2

# ---------------------------------------------------------------------------
# Build KV transfer configs
# ---------------------------------------------------------------------------
if [[ "${CONNECTOR}" == "pd_connector" ]]; then
    PREFILLER_KV_CONFIG="{\"kv_connector\":\"OffloadingConnector\",\"kv_role\":\"kv_both\",\"kv_connector_extra_config\":{\"spec_name\":\"TieringOffloadingSpec\",\"cpu_bytes_to_use\":${CPU_BYTES},\"secondary_tiers\":[{\"type\":\"pd_connector\",\"host\":\"127.0.0.1\",\"port\":${PREFILLER_PD_PORT}}]}}"
    DECODER_KV_CONFIG="{\"kv_connector\":\"OffloadingConnector\",\"kv_role\":\"kv_both\",\"kv_connector_extra_config\":{\"spec_name\":\"TieringOffloadingSpec\",\"cpu_bytes_to_use\":${CPU_BYTES},\"secondary_tiers\":[{\"type\":\"pd_connector\",\"host\":\"127.0.0.1\",\"port\":${DECODER_PD_PORT}}]}}"
else
    PREFILLER_KV_CONFIG="{\"kv_connector\":\"NixlConnector\",\"kv_role\":\"kv_both\"}"
    DECODER_KV_CONFIG="{\"kv_connector\":\"NixlConnector\",\"kv_role\":\"kv_both\"}"
fi

# ---------------------------------------------------------------------------
# Start Prefiller
# ---------------------------------------------------------------------------
echo "=== Starting Prefiller ==="
CUDA_VISIBLE_DEVICES=${PREFILLER_GPUS} \
VLLM_LOGGING_LEVEL=${LOG_LEVEL} \
PYTHONHASHSEED=42 \
VLLM_NIXL_SIDE_CHANNEL_PORT=${NIXL_SIDE_CHANNEL_PORT_PREFILLER} \
    ${VLLM_BIN} serve "${MODEL}" \
        --port "${PREFILLER_HTTP_PORT}" \
        --enforce-eager \
        --block-size "${BLOCK_SIZE}" \
        --gpu-memory-utilization "${GPU_MEM_UTIL}" \
        --max-model-len "${MAX_MODEL_LEN}" \
        --kv-transfer-config "${PREFILLER_KV_CONFIG}" \
        > "${PREFILLER_LOG}" 2>&1 &
PREFILLER_PID=$!
PIDS+=("$PREFILLER_PID")
echo "  PID ${PREFILLER_PID} → ${PREFILLER_LOG}"

# ---------------------------------------------------------------------------
# Start Decoder
# ---------------------------------------------------------------------------
echo "=== Starting Decoder ==="
CUDA_VISIBLE_DEVICES=${DECODER_GPUS} \
VLLM_LOGGING_LEVEL=${LOG_LEVEL} \
PYTHONHASHSEED=42 \
VLLM_NIXL_SIDE_CHANNEL_PORT=${NIXL_SIDE_CHANNEL_PORT_DECODER} \
    ${VLLM_BIN} serve "${MODEL}" \
        --port "${DECODER_HTTP_PORT}" \
        --enforce-eager \
        --block-size "${BLOCK_SIZE}" \
        --gpu-memory-utilization "${GPU_MEM_UTIL}" \
        --max-model-len "${MAX_MODEL_LEN}" \
        --kv-transfer-config "${DECODER_KV_CONFIG}" \
        > "${DECODER_LOG}" 2>&1 &
DECODER_PID=$!
PIDS+=("$DECODER_PID")
echo "  PID ${DECODER_PID} → ${DECODER_LOG}"

# ---------------------------------------------------------------------------
# Wait for health
# ---------------------------------------------------------------------------
wait_for_health() {
    local name="$1" port="$2" path="${3:-/health}" logfile="$4"
    local deadline=$(( $(date +%s) + HEALTH_TIMEOUT ))
    echo -n "Waiting for ${name} (127.0.0.1:${port}) ..."
    while true; do
        if curl -sf "http://127.0.0.1:${port}${path}" > /dev/null 2>&1; then
            echo " ready"
            return 0
        fi
        if [[ "$(date +%s)" -ge "${deadline}" ]]; then
            echo ""
            echo "ERROR: ${name} did not become healthy within ${HEALTH_TIMEOUT}s" >&2
            echo "--- last 20 lines of log ---" >&2
            tail -20 "${logfile}" 2>/dev/null >&2 || true
            return 1
        fi
        sleep 5
        echo -n "."
    done
}

wait_for_health "Prefiller" "${PREFILLER_HTTP_PORT}" "/health" "${PREFILLER_LOG}" || exit 1
wait_for_health "Decoder"   "${DECODER_HTTP_PORT}"   "/health" "${DECODER_LOG}"   || exit 1

# ---------------------------------------------------------------------------
# Start Proxy
# ---------------------------------------------------------------------------
echo "=== Starting Proxy ==="
if [[ "${CONNECTOR}" == "pd_connector" ]]; then
    _DECODER_FIRST_FLAG=""
    [[ "${DECODER_FIRST}" == "true" ]] && _DECODER_FIRST_FLAG="--decoder-first"
    ${PYTHON_BIN} "${SCRIPT_DIR}/pd_connector_proxy.py" \
        --port "${PROXY_PORT}" \
        --host 127.0.0.1 \
        --prefiller-hosts 127.0.0.1 \
        --prefiller-ports "${PREFILLER_HTTP_PORT}" \
        --decoder-hosts 127.0.0.1 \
        --decoder-ports "${DECODER_HTTP_PORT}" \
        --pd-connector-host 127.0.0.1 \
        --pd-connector-port "${PREFILLER_PD_PORT}" \
        ${_DECODER_FIRST_FLAG} \
        > "${PROXY_LOG}" 2>&1 &
else
    ${PYTHON_BIN} "${REPO_ROOT}/tests/v1/kv_connector/nixl_integration/toy_proxy_server.py" \
        --port "${PROXY_PORT}" \
        --host 127.0.0.1 \
        --prefiller-hosts 127.0.0.1 \
        --prefiller-ports "${PREFILLER_HTTP_PORT}" \
        --decoder-hosts 127.0.0.1 \
        --decoder-ports "${DECODER_HTTP_PORT}" \
        > "${PROXY_LOG}" 2>&1 &
fi
PROXY_PID=$!
PIDS+=("$PROXY_PID")
echo "  PID ${PROXY_PID} → ${PROXY_LOG}"

wait_for_health "Proxy" "${PROXY_PORT}" "/healthcheck" "${PROXY_LOG}" || exit 1

# ---------------------------------------------------------------------------
# Write state file (consumed by bench_sweep.sh)
# ---------------------------------------------------------------------------
cat > "${STATE_FILE}" <<STATE
# Generated by deploy_local.sh — $(date)
CONNECTOR=${CONNECTOR}
PREFILLER_ADDR=127.0.0.1
DECODER_ADDR=127.0.0.1
PREFILLER_HTTP_PORT=${PREFILLER_HTTP_PORT}
DECODER_HTTP_PORT=${DECODER_HTTP_PORT}
PROXY_PORT=${PROXY_PORT}
PREFILLER_PID=${PREFILLER_PID}
DECODER_PID=${DECODER_PID}
PROXY_PID=${PROXY_PID}
MODEL=${MODEL}
PROXY_URL=http://127.0.0.1:${PROXY_PORT}
STATE

cat <<SUMMARY

=== Deploy complete: ${CONNECTOR} ===
  Model:    ${MODEL}  gpu_mem=${GPU_MEM_UTIL}  max_len=${MAX_MODEL_LEN}
  Proxy:    http://127.0.0.1:${PROXY_PORT}/v1/completions
  State:    ${STATE_FILE}
  Logs:     ${PREFILLER_LOG}  ${DECODER_LOG}  ${PROXY_LOG}

  To stop:  kill ${PREFILLER_PID} ${DECODER_PID} ${PROXY_PID}
            (or Ctrl-C — cleanup trap will handle it)

  Example request:
    curl http://127.0.0.1:${PROXY_PORT}/v1/completions \\
      -H 'Content-Type: application/json' \\
      -d '{"model":"${MODEL}","prompt":"Hello","max_tokens":20}'

SUMMARY

# Keep the script alive so the trap can clean up on Ctrl-C
echo "Press Ctrl-C to stop all services."
wait
