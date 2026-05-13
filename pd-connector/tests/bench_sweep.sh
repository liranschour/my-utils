#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the vLLM project
#
# Run vllm bench serve at increasing concurrency levels against a deployed
# PD-disaggregated setup. Reads connection info from a deploy state file
# (written by deploy.sh) and a config file.
#
# Usage:
#   bash bench_sweep.sh --config configs/cluster_pd.env
#   bash bench_sweep.sh --config configs/cluster_nixl.env
#
# The script runs on the local machine and shells into the pod via oc exec
# to run bench serve (so bench traffic never crosses a network hop).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

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
    echo "Usage: bash bench_sweep.sh --config <config_file>" >&2
    exit 1
fi

# Source config with env-precedence semantics (same as deploy.sh)
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
PROXY_POD="${PROXY_POD:-llmd-transport-decoder}"
PROXY_PORT="${PROXY_PORT:-8192}"
MODEL="${MODEL:-Qwen/Qwen3-8B}"
STATE_FILE="${STATE_FILE:-/tmp/deploy_state.env}"

# Sweep params (can be overridden by env)
VLLM_BIN="${VLLM_BIN:-/workspace/venv/bin/vllm}"

CONCURRENCY_LEVELS="${CONCURRENCY_LEVELS:-1 2 4 8 16 32 64}"
SATURATION_PCT="${SATURATION_PCT:-5}"      # stop when throughput grows < this %
NUM_PROMPTS="${NUM_PROMPTS:-200}"
RANDOM_INPUT_LEN="${RANDOM_INPUT_LEN:-512}"
RANDOM_OUTPUT_LEN="${RANDOM_OUTPUT_LEN:-128}"

TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
RESULT_DIR="${RESULT_DIR:-${SCRIPT_DIR}/results/${CONNECTOR}/${TIMESTAMP}}"

echo "=== Bench Sweep: ${CONNECTOR} ==="
echo "  Proxy:         pod=${PROXY_POD} port=${PROXY_PORT}"
echo "  Model:         ${MODEL}"
echo "  Concurrencies: ${CONCURRENCY_LEVELS}"
echo "  Prompts/run:   ${NUM_PROMPTS}  input=${RANDOM_INPUT_LEN}  output=${RANDOM_OUTPUT_LEN}"
echo "  Results dir:   ${RESULT_DIR}"
echo ""

# Create result dir on the pod
oc exec "${PROXY_POD}" -- mkdir -p "${RESULT_DIR}"

# ---------------------------------------------------------------------------
# Sweep
# ---------------------------------------------------------------------------
prev_throughput=0
max_concurrency_reached=""

for CONC in ${CONCURRENCY_LEVELS}; do
    SEED="$(date +%s)"
    RUN_LABEL="conc${CONC}"
    echo "--- Concurrency ${CONC} (seed=${SEED}) ---"

    # Run bench inside the pod so traffic stays local
    oc exec "${PROXY_POD}" -- bash -c "
        cd /tmp && ${VLLM_BIN} bench serve \
            --host 127.0.0.1 \
            --port ${PROXY_PORT} \
            --model '${MODEL}' \
            --dataset-name random \
            --random-input-len ${RANDOM_INPUT_LEN} \
            --random-output-len ${RANDOM_OUTPUT_LEN} \
            --num-prompts ${NUM_PROMPTS} \
            --seed ${SEED} \
            --max-concurrency ${CONC} \
            --save-result \
            --result-dir '${RESULT_DIR}' \
            2>&1
    "

    # Extract throughput from the most recently written JSON in the result dir
    THROUGHPUT=$(oc exec "${PROXY_POD}" -- bash -c "
        latest=\$(ls -t '${RESULT_DIR}'/*.json 2>/dev/null | head -1)
        if [[ -n \"\$latest\" ]]; then
            python3 -c \"
import json, sys
with open('\$latest') as f:
    d = json.load(f)
print(d.get('request_throughput', 0))
\"
        else
            echo 0
        fi
    " 2>/dev/null || echo 0)

    echo "  Throughput: ${THROUGHPUT} req/s"

    # Saturation check (skip for first level)
    if [[ "${prev_throughput}" != "0" ]]; then
        if ! oc exec "${PROXY_POD}" -- python3 -c "
prev=${prev_throughput}; cur=${THROUGHPUT}
pct = (cur - prev) / prev * 100 if prev > 0 else 100
print(f'  Throughput growth: {pct:.1f}%')
exit(1 if pct < ${SATURATION_PCT} else 0)
" 2>&1; then
            max_concurrency_reached="${CONC}"
            echo "  Saturation at concurrency ${CONC} — stopping sweep"
            break
        fi
    fi

    prev_throughput="${THROUGHPUT}"
done

# ---------------------------------------------------------------------------
# Copy results back to local machine
# ---------------------------------------------------------------------------
LOCAL_RESULT_DIR="${SCRIPT_DIR}/results/${CONNECTOR}/${TIMESTAMP}"
mkdir -p "${LOCAL_RESULT_DIR}"
oc cp "${PROXY_POD}:${RESULT_DIR}" "${LOCAL_RESULT_DIR}/" 2>/dev/null || true

echo ""
echo "=== Sweep complete: ${CONNECTOR} ==="
echo "  Results on pod:    ${RESULT_DIR}"
echo "  Results local:     ${LOCAL_RESULT_DIR}"
[[ -n "${max_concurrency_reached}" ]] && \
    echo "  Saturation point:  concurrency=${max_concurrency_reached}"
echo ""
echo "  To compare results:"
echo "    python3 ${SCRIPT_DIR}/collect_results.py ${SCRIPT_DIR}/results/"
