#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the vLLM project
#
# Compare PDConnector vs NixlConnector across a (concurrency x input_len) grid.
#
# For each connector: deploys via deploy.sh, then runs `vllm bench serve` for
# every cell of the grid with a unique seed, saving the per-run JSON. After
# both connectors finish, aggregates results into a CSV and prints a side-by-
# side markdown comparison table.
#
# Usage:
#   bash bench_compare.sh --pod llmd-decoder
#   bash bench_compare.sh --pod llmd-decoder --concurrency 1,16 --input-len 1024 --num-prompts 50

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
POD="llmd-decoder"
MODEL="Qwen/Qwen3-8B"
GPU_MEM_UTIL="0.90"
MAX_MODEL_LEN="40960"
BLOCK_SIZE="16"
CPU_BYTES="68719476736"     # 64 GB; only used for pd_connector
CONCURRENCY_LIST="1,16,100"
INPUT_LEN_LIST="1024,4096"
OUTPUT_LEN="1"
NUM_PROMPTS="200"
CONNECTOR_LIST="pd_connector,pd_decoder_first,nixl"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
RESULTS_DIR="${SCRIPT_DIR}/results/compare_${TIMESTAMP}"
PROXY_PORT="8192"
VLLM_BIN="vllm"
PYTHON_BIN="python3"

# ---------------------------------------------------------------------------
# Arg parse
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --pod)             POD="$2"; shift 2 ;;
        --model)           MODEL="$2"; shift 2 ;;
        --gpu-mem-util)    GPU_MEM_UTIL="$2"; shift 2 ;;
        --max-model-len)   MAX_MODEL_LEN="$2"; shift 2 ;;
        --block-size)      BLOCK_SIZE="$2"; shift 2 ;;
        --cpu-bytes)       CPU_BYTES="$2"; shift 2 ;;
        --concurrency)     CONCURRENCY_LIST="$2"; shift 2 ;;
        --input-len)       INPUT_LEN_LIST="$2"; shift 2 ;;
        --output-len)      OUTPUT_LEN="$2"; shift 2 ;;
        --num-prompts)     NUM_PROMPTS="$2"; shift 2 ;;
        --connectors)      CONNECTOR_LIST="$2"; shift 2 ;;
        --results-dir)     RESULTS_DIR="$2"; shift 2 ;;
        -h|--help)
            grep '^#' "$0" | sed 's/^# \?//'
            exit 0
            ;;
        *) echo "Unknown argument: $1" >&2; exit 1 ;;
    esac
done

# Comma-separated -> space-separated for shell iteration.
CONCURRENCIES="${CONCURRENCY_LIST//,/ }"
INPUT_LENS="${INPUT_LEN_LIST//,/ }"
CONNECTORS="${CONNECTOR_LIST//,/ }"

mkdir -p "${RESULTS_DIR}"

cat <<HEADER
=== bench_compare.sh ===
  Pod:           ${POD}
  Model:         ${MODEL}
  Connectors:    ${CONNECTORS}
  Concurrency:   ${CONCURRENCIES}
  Input lens:    ${INPUT_LENS}
  Output len:    ${OUTPUT_LEN}
  Prompts/cell:  ${NUM_PROMPTS}
  Results dir:   ${RESULTS_DIR}
HEADER

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
deploy_connector() {
    local connector="$1"
    local config_file real_connector decoder_first
    case "${connector}" in
        pd_connector)
            real_connector="pd_connector"
            decoder_first="false"
            config_file="${SCRIPT_DIR}/configs/cluster_pd.env" ;;
        pd_decoder_first)
            real_connector="pd_connector"
            decoder_first="true"
            config_file="${SCRIPT_DIR}/configs/cluster_pd.env" ;;
        nixl)
            real_connector="nixl"
            decoder_first="false"
            config_file="${SCRIPT_DIR}/configs/cluster_nixl.env" ;;
        *)
            echo "Unknown setup: ${connector}" >&2
            return 1 ;;
    esac

    echo ""
    echo "############################################################"
    echo "# Deploying ${connector} (CONNECTOR=${real_connector} DECODER_FIRST=${decoder_first})"
    echo "############################################################"

    CONNECTOR="${real_connector}" \
    DECODER_FIRST="${decoder_first}" \
    PREFILLER_POD="${POD}" DECODER_POD="${POD}" PROXY_POD="${POD}" \
    MODEL="${MODEL}" \
    GPU_MEM_UTIL="${GPU_MEM_UTIL}" \
    MAX_MODEL_LEN="${MAX_MODEL_LEN}" \
    BLOCK_SIZE="${BLOCK_SIZE}" \
    CPU_BYTES="${CPU_BYTES}" \
    VLLM_BIN="${VLLM_BIN}" \
    PYTHON_BIN="${PYTHON_BIN}" \
    bash "${SCRIPT_DIR}/deploy.sh" --config "${config_file}"
}

# Run one bench cell and copy the resulting JSON back locally.
# $1 connector, $2 concurrency, $3 input_len, $4 seed, $5 local out dir
run_cell() {
    local connector="$1" conc="$2" input_len="$3" seed="$4" out_dir="$5"
    local pod_dir="/tmp/bench_compare_${TIMESTAMP}/${connector}/c${conc}_i${input_len}"

    echo ""
    echo "--- ${connector}  concurrency=${conc}  input_len=${input_len}  seed=${seed} ---"
    oc exec "${POD}" -- mkdir -p "${pod_dir}"

    oc exec "${POD}" -- "${VLLM_BIN}" bench serve \
        --backend vllm \
        --model "${MODEL}" \
        --endpoint /v1/completions \
        --base-url "http://127.0.0.1:${PROXY_PORT}" \
        --dataset-name random \
        --random-input-len "${input_len}" \
        --random-output-len "${OUTPUT_LEN}" \
        --num-prompts "${NUM_PROMPTS}" \
        --max-concurrency "${conc}" \
        --request-rate inf \
        --seed "${seed}" \
        --save-result \
        --result-dir "${pod_dir}" \
        --metadata "connector=${connector}" "input_len=${input_len}"

    mkdir -p "${out_dir}"
    oc cp "${POD}:${pod_dir}" "${out_dir}/" 2>/dev/null || \
        echo "WARN: failed to copy ${pod_dir} from pod" >&2
}

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
run_index=0
BASE_SEED="$(date +%s)"

for connector in ${CONNECTORS}; do
    deploy_connector "${connector}"

    # quick health sanity (PD proxy exposes /healthcheck; nixl proxy exposes /healthcheck too)
    if ! oc exec "${POD}" -- curl -sf "http://127.0.0.1:${PROXY_PORT}/healthcheck" >/dev/null 2>&1; then
        echo "ERROR: proxy unhealthy after deploying ${connector}" >&2
        exit 1
    fi

    local_out="${RESULTS_DIR}/${connector}"
    mkdir -p "${local_out}"

    for input_len in ${INPUT_LENS}; do
        for conc in ${CONCURRENCIES}; do
            run_index=$(( run_index + 1 ))
            seed=$(( BASE_SEED + run_index ))   # never 0
            run_cell "${connector}" "${conc}" "${input_len}" "${seed}" "${local_out}"
        done
    done
done

# ---------------------------------------------------------------------------
# Aggregate + print comparison
# ---------------------------------------------------------------------------
echo ""
echo "############################################################"
echo "# Aggregating results"
echo "############################################################"

CONNECTORS_STR="${CONNECTORS}" \
RESULTS_DIR="${RESULTS_DIR}" \
MODEL="${MODEL}" \
NUM_PROMPTS="${NUM_PROMPTS}" \
OUTPUT_LEN="${OUTPUT_LEN}" \
python3 - <<'PYEOF'
import csv
import json
import os
from pathlib import Path

connectors = os.environ["CONNECTORS_STR"].split()
results_dir = Path(os.environ["RESULTS_DIR"])
model = os.environ["MODEL"]
num_prompts = os.environ["NUM_PROMPTS"]
output_len = os.environ["OUTPUT_LEN"]

# Map (connector, concurrency, input_len) -> metrics dict
rows = {}

def _coerce(v):
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None

for connector in connectors:
    cdir = results_dir / connector
    if not cdir.is_dir():
        print(f"WARN: missing results dir {cdir}")
        continue
    # Walk every JSON under the connector dir (per-cell subdirs from oc cp).
    for jpath in cdir.rglob("*.json"):
        try:
            with open(jpath) as f:
                d = json.load(f)
        except Exception as e:
            print(f"WARN: failed to read {jpath}: {e}")
            continue
        conc = int(d.get("max_concurrency") or 0)
        # bench_compare passes --metadata connector=<c> input_len=<n>;
        # vllm bench serve flattens these into top-level JSON keys.
        try:
            input_len_val = int(d.get("input_len"))
        except (TypeError, ValueError):
            # fallback: parent dir name c<conc>_i<input_len>
            try:
                input_len_val = int(jpath.parent.name.split("_i")[-1])
            except ValueError:
                input_len_val = -1
        connector_tag = d.get("connector") or connector
        key = (connector_tag, conc, input_len_val)
        rows[key] = {
            "connector": connector_tag,
            "concurrency": conc,
            "input_len": input_len_val,
            "output_len": int(output_len),
            "num_prompts": int(num_prompts),
            "duration_s": _coerce(d.get("duration")),
            "completed": int(d.get("completed") or 0),
            "failed": int(d.get("failed") or 0),
            "req_throughput": _coerce(d.get("request_throughput")),
            "output_tok_s": _coerce(d.get("output_throughput")),
            "total_tok_s": _coerce(d.get("total_token_throughput")),
            "mean_ttft_ms": _coerce(d.get("mean_ttft_ms")),
            "p99_ttft_ms":  _coerce(d.get("p99_ttft_ms")),
            "mean_tpot_ms": _coerce(d.get("mean_tpot_ms")),
            "p99_tpot_ms":  _coerce(d.get("p99_tpot_ms")),
            "mean_itl_ms":  _coerce(d.get("mean_itl_ms")),
            "p99_itl_ms":   _coerce(d.get("p99_itl_ms")),
        }

# ---------- Write CSV ----------
csv_path = results_dir / "summary.csv"
fields = ["connector","concurrency","input_len","output_len","num_prompts",
          "duration_s","completed","failed",
          "req_throughput","output_tok_s","total_tok_s",
          "mean_ttft_ms","p99_ttft_ms",
          "mean_tpot_ms","p99_tpot_ms",
          "mean_itl_ms","p99_itl_ms"]
with open(csv_path, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=fields)
    w.writeheader()
    for key in sorted(rows.keys()):
        w.writerow(rows[key])
print(f"Wrote {csv_path}  ({len(rows)} rows)")

# ---------- Markdown comparison table ----------
# Pivot: for each (concurrency, input_len), one row per metric, columns per
# connector + delta vs first connector.
metrics = [
    ("req_throughput",  "req/s",      "higher"),
    ("output_tok_s",    "out tok/s",  "higher"),
    ("total_tok_s",     "total tok/s","higher"),
    ("mean_ttft_ms",    "TTFT mean ms","lower"),
    ("p99_ttft_ms",     "TTFT p99 ms","lower"),
    ("mean_tpot_ms",    "TPOT mean ms","lower"),
    ("p99_tpot_ms",     "TPOT p99 ms","lower"),
    ("mean_itl_ms",     "ITL mean ms","lower"),
    ("p99_itl_ms",      "ITL p99 ms","lower"),
]

cells = sorted({(c, il) for (_, c, il) in rows.keys()})
n_conn = len(connectors)
base = connectors[0]

md_lines = []
md_lines.append(f"# Comparison: {' vs '.join(connectors)}")
md_lines.append("")
md_lines.append(f"- Model: `{model}`")
md_lines.append(f"- num_prompts: {num_prompts}")
md_lines.append(f"- output_len: {output_len}")
md_lines.append("")

# Header
hdr = ["concurrency", "input_len", "metric"] + connectors
if n_conn > 1:
    for c in connectors[1:]:
        hdr.append(f"Δ ({c}-{base})/{base}")
md_lines.append("| " + " | ".join(hdr) + " |")
md_lines.append("|" + "|".join(["---"] * len(hdr)) + "|")

def fmt(v):
    if v is None:
        return "—"
    if abs(v) >= 100:
        return f"{v:.1f}"
    return f"{v:.2f}"

warnings = []
for (conc, il) in cells:
    for mkey, mlabel, _direction in metrics:
        cells_vals = []
        for connector in connectors:
            r = rows.get((connector, conc, il))
            v = r[mkey] if r else None
            cells_vals.append(v)
        # skip metric rows where every connector reported None
        if all(v is None for v in cells_vals):
            continue
        line = [str(conc), str(il), mlabel]
        line += [fmt(v) for v in cells_vals]
        if n_conn > 1:
            base_v = cells_vals[0]
            for v in cells_vals[1:]:
                if base_v in (None, 0) or v is None:
                    line.append("—")
                else:
                    line.append(f"{(v - base_v) / base_v * 100:+.1f}%")
        md_lines.append("| " + " | ".join(line) + " |")
    # cell warnings: failed > 0 in any connector
    for connector in connectors:
        r = rows.get((connector, conc, il))
        if r and r.get("failed"):
            warnings.append(f"{connector} c={conc} i={il}: {r['failed']} failed requests")

if warnings:
    md_lines.append("")
    md_lines.append("## Warnings")
    md_lines.append("")
    for w in warnings:
        md_lines.append(f"- {w}")

md_path = results_dir / "summary.md"
with open(md_path, "w") as f:
    f.write("\n".join(md_lines) + "\n")

# Echo the same table to stdout.
print()
print("\n".join(md_lines))
print()
print(f"Raw JSONs: {results_dir}")
print(f"CSV:       {csv_path}")
print(f"Markdown:  {md_path}")
PYEOF

echo ""
echo "=== bench_compare.sh complete ==="
echo "  Results: ${RESULTS_DIR}"
