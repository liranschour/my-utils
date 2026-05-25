---
name: bench-pd
description: Run vllm bench serve against a deployed PD connector environment. Use when the user asks to "bench pd", "run pd benchmark", "bench serve", or "run perf test".
---

# Bench PD Connector

Run `vllm bench serve` against the currently deployed PD connector proxy and report results.

## Arguments

- `$ARGUMENTS` — `[--concurrency N] [--input-len N] [--output-len N] [--num-prompts N] [--runs N]`
  - `--concurrency` (default: 1) — max concurrent requests
  - `--input-len` (default: 256) — random input token length
  - `--output-len` (default: 64) — random output token length
  - `--num-prompts` (default: 20) — number of prompts per run
  - `--runs` (default: 1) — number of repeated runs with unique seeds

## Steps

1. **Parse arguments**: Extract parameters from `$ARGUMENTS`, applying defaults for any not specified.

2. **Read deploy state**: Get the pod name and model from the deploy state file:
   ```
   oc exec <pod_name> -- cat /tmp/deploy_state.env
   ```
   Extract `PROXY_POD`, `MODEL`, and proxy port (default 8192). If the file doesn't exist, ask the user to deploy first.

3. **Verify proxy is up**:
   ```
   oc exec <pod_name> -- curl -sf http://127.0.0.1:8192/health
   ```
   If not healthy, inform the user the proxy is down.

4. **Run benchmark**: Execute the bench for each run with a unique seed. Generate the seed using the current epoch timestamp (seconds) plus the run index — NEVER use seed 0. Use `--request-rate inf` to let concurrency control throughput:
   ```
   SEED=$(( $(date +%s) + run_index ))
   oc exec <pod_name> -- vllm bench serve \
     --backend vllm \
     --model <model> \
     --endpoint /v1/completions \
     --base-url http://127.0.0.1:8192 \
     --dataset-name random \
     --random-input-len <input_len> \
     --random-output-len <output_len> \
     --num-prompts <num_prompts> \
     --max-concurrency <concurrency> \
     --request-rate inf \
     --seed $SEED
   ```

5. **Report results**: Present a summary table with one row per run:

   | Run | Seed | Success/Fail | Duration | Req Throughput | Output tok/s | Mean TTFT | Mean TPOT |
   |-----|------|--------------|----------|----------------|--------------|-----------|-----------|

   After the table, show averages across all runs (excluding any failed runs).

   If any run has failed requests, show the failure count and suggest redeploying with `--debug` to investigate.
