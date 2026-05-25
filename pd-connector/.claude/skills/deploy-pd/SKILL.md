---
name: deploy-pd
description: This skill should be used when the user asks to "deploy pd", "deploy pd connector", "deploy prefill-decode", "deploy nixl", "deploy nixl connector", "run pd sanity test", "deploy vllm on pod", or needs to deploy a PD-disaggregated vLLM environment on an OC pod and verify it works.
---

# Deploy PD / NIXL Connector on OC Pod

Deploy a PD-disaggregated vLLM environment (prefiller + decoder + proxy) on an OC pod and verify it works. Supports both PDConnector and NixlConnector.

## Arguments

- `$ARGUMENTS` — `<pod_name> [--decoder-pod <name>] [--large] [--nixl] [--debug]`
  - `pod_name` (required) — prefiller + proxy pod (single-pod mode: also hosts the decoder)
  - `--decoder-pod <name>` (optional) — second pod hosting the decoder. Triggers two-pod mode (proxy stays co-located with prefiller on `pod_name`).
  - `--large` (optional) — use the large model preset instead of the default small one
  - `--nixl` (optional) — deploy with NixlConnector (GPU-to-GPU) instead of PDConnector (CPU-tier based)
  - `--debug` (optional) — deploy with `LOG_LEVEL=DEBUG` for verbose vllm logging to help diagnose failures

## Model Presets

| Preset | Model | GPU Mem | Max Len | Block Size | `CPU_BYTES` (single-pod) | `CPU_BYTES` (two-pod) |
|--------|-------|---------|---------|------------|--------------------------|-----------------------|
| **small** (default) | Qwen/Qwen3-0.6B | 0.90 | 32768 | 16 | `2147483648` (2 GiB) | `103079215104` (96 GiB) |
| **large** (`--large`) | Qwen/Qwen3-8B | 0.90 | 40960 | 16 | `68719476736` (64 GiB) | `103079215104` (96 GiB) |

Two-pod `CPU_BYTES` is sized to **exceed the per-engine GPU KV cache** (~69 GiB
for small, ~55 GiB for large on an 80 GB GPU at `gpu_memory_utilization=0.90`).
The CPU tier must be larger than the GPU tier — otherwise it can't absorb GPU
evictions and the offload layer is useless. Single-pod values are smaller
because two engines share `/dev/shm` on the same pod. NixlConnector ignores
`CPU_BYTES` (GPU-to-GPU transport).

## Connector Differences

| | PDConnector (default) | NixlConnector (`--nixl`) |
|---|---|---|
| **CONNECTOR env** | `pd_connector` | `nixl` |
| **KV config** | `OffloadingConnector` + secondary tiers + CPU bytes | `NixlConnector` with `kv_role=kv_both` |
| **Proxy** | `pd_connector_proxy.py` | `toy_proxy_server.py` |
| **CPU bytes** | Required (set from preset) | Not used (GPU-to-GPU) |
| **Base config** | `cluster_pd.env` | `cluster_nixl.env` |

## Steps

1. **Parse arguments**: Extract `pod_name`, `--decoder-pod <name>` (if present), and the flags `--large`, `--nixl`, `--debug` from `$ARGUMENTS`.

   - If `--decoder-pod <name>` is set: **two-pod mode**. Both pods host one engine each; proxy stays on `pod_name` (co-located with the prefiller). Each pod uses its own GPU 0.
   - Otherwise: **single-pod mode** (current behavior — prefiller on GPU 0, decoder on GPU 1 of `pod_name`).

2. **Resolve config**: Based on connector, preset, and topology:

   **PDConnector — single-pod (default):**
   - **small**: Look for `tests/v1/kv_offload/configs/cluster_pd_<pod_name>.env` (replacing dashes with underscores). If it doesn't exist, use env var overrides with the small preset values.
   - **large** (`--large`):
     ```
     PREFILLER_POD=<pod_name> DECODER_POD=<pod_name> PROXY_POD=<pod_name> \
     MODEL=Qwen/Qwen3-8B GPU_MEM_UTIL=0.90 MAX_MODEL_LEN=40960 BLOCK_SIZE=16 \
     CPU_BYTES=68719476736 VLLM_BIN=vllm PYTHON_BIN=python3 \
     bash tests/v1/kv_offload/deploy.sh --config tests/v1/kv_offload/configs/cluster_pd.env
     ```

   **PDConnector — two-pod (`--decoder-pod <decoder_pod>`):**
   - **small**:
     ```
     PREFILLER_POD=<pod_name> DECODER_POD=<decoder_pod> PROXY_POD=<pod_name> \
     PREFILLER_GPUS=0 DECODER_GPUS=0 \
     MODEL=Qwen/Qwen3-0.6B GPU_MEM_UTIL=0.90 MAX_MODEL_LEN=32768 BLOCK_SIZE=16 \
     CPU_BYTES=103079215104 VLLM_BIN=vllm PYTHON_BIN=python3 \
     bash tests/v1/kv_offload/deploy.sh --config tests/v1/kv_offload/configs/cluster_pd_two_pods.env
     ```
   - **large** (`--large --decoder-pod ...`):
     ```
     PREFILLER_POD=<pod_name> DECODER_POD=<decoder_pod> PROXY_POD=<pod_name> \
     PREFILLER_GPUS=0 DECODER_GPUS=0 \
     MODEL=Qwen/Qwen3-8B GPU_MEM_UTIL=0.90 MAX_MODEL_LEN=40960 BLOCK_SIZE=16 \
     CPU_BYTES=103079215104 VLLM_BIN=vllm PYTHON_BIN=python3 \
     bash tests/v1/kv_offload/deploy.sh --config tests/v1/kv_offload/configs/cluster_pd_two_pods.env
     ```

   **NixlConnector — single-pod (`--nixl`):**
   - **small**:
     ```
     CONNECTOR=nixl PREFILLER_POD=<pod_name> DECODER_POD=<pod_name> PROXY_POD=<pod_name> \
     MODEL=Qwen/Qwen3-0.6B GPU_MEM_UTIL=0.90 MAX_MODEL_LEN=32768 BLOCK_SIZE=16 \
     VLLM_BIN=vllm PYTHON_BIN=python3 \
     bash tests/v1/kv_offload/deploy.sh --config tests/v1/kv_offload/configs/cluster_nixl.env
     ```
   - **large** (`--large --nixl`):
     ```
     CONNECTOR=nixl PREFILLER_POD=<pod_name> DECODER_POD=<pod_name> PROXY_POD=<pod_name> \
     MODEL=Qwen/Qwen3-8B GPU_MEM_UTIL=0.90 MAX_MODEL_LEN=40960 BLOCK_SIZE=16 \
     VLLM_BIN=vllm PYTHON_BIN=python3 \
     bash tests/v1/kv_offload/deploy.sh --config tests/v1/kv_offload/configs/cluster_nixl.env
     ```

   **NixlConnector — two-pod (`--nixl --decoder-pod <decoder_pod>`):**
   - **small**:
     ```
     CONNECTOR=nixl PREFILLER_POD=<pod_name> DECODER_POD=<decoder_pod> PROXY_POD=<pod_name> \
     PREFILLER_GPUS=0 DECODER_GPUS=0 \
     MODEL=Qwen/Qwen3-0.6B GPU_MEM_UTIL=0.90 MAX_MODEL_LEN=32768 BLOCK_SIZE=16 \
     VLLM_BIN=vllm PYTHON_BIN=python3 \
     bash tests/v1/kv_offload/deploy.sh --config tests/v1/kv_offload/configs/cluster_nixl_two_pods.env
     ```
   - **large** (`--large --nixl --decoder-pod ...`):
     ```
     CONNECTOR=nixl PREFILLER_POD=<pod_name> DECODER_POD=<decoder_pod> PROXY_POD=<pod_name> \
     PREFILLER_GPUS=0 DECODER_GPUS=0 \
     MODEL=Qwen/Qwen3-8B GPU_MEM_UTIL=0.90 MAX_MODEL_LEN=40960 BLOCK_SIZE=16 \
     VLLM_BIN=vllm PYTHON_BIN=python3 \
     bash tests/v1/kv_offload/deploy.sh --config tests/v1/kv_offload/configs/cluster_nixl_two_pods.env
     ```

   Note: No `CPU_BYTES` for nixl — it does GPU-to-GPU transfer.

3. **Pre-flight checks** (run these in parallel; in two-pod mode run on **both** `<pod_name>` and `<decoder_pod>`):
   - Verify the pod is running: `oc get pod <pod>`
   - Verify GPUs are available: `oc exec <pod> -- nvidia-smi --query-gpu=memory.free --format=csv,noheader`
   - Verify vllm is installed: `oc exec <pod> -- python3 -c "import vllm; print(vllm.__version__)"`

4. **Sync vllm source code**: Ensure each pod's vllm source at `/vllm-workspace/vllm` matches the local working directory. In two-pod mode run **all three sub-steps on both pods**.

   a. **Check branch**: Verify each pod is on the same branch as local:
      ```
      oc exec <pod> -- bash -c "cd /vllm-workspace/vllm && git branch --show-current"
      ```
      Compare with the local branch. If different, fetch and checkout:
      ```
      oc exec <pod> -- bash -c "cd /vllm-workspace/vllm && git fetch origin <branch> && git checkout <branch>"
      ```

   b. **Check commits**: Compare local HEAD with each pod's HEAD:
      ```
      # local
      git rev-parse HEAD
      # pod
      oc exec <pod> -- bash -c "cd /vllm-workspace/vllm && git rev-parse HEAD"
      ```
      If they differ, pull on the pod:
      ```
      oc exec <pod> -- bash -c "cd /vllm-workspace/vllm && git pull origin <branch>"
      ```

   c. **Sync uncommitted changes**: Check for locally modified (dirty) files:
      ```
      git diff --name-only HEAD
      ```
      For each modified file, copy it to each pod at the matching path under `/vllm-workspace/vllm/`:
      ```
      oc cp <local_file> <pod>:/vllm-workspace/vllm/<relative_path>
      ```
      Report which files were synced (and to which pod).

5. **Copy scripts** to the proxy/prefiller pod (`pod_name`). The proxy lives there in both topologies, so no decoder-pod copy is needed:
   ```
   oc cp tests/v1/kv_offload/pd_connector_proxy.py <pod_name>:/tmp/pd_connector_proxy.py
   oc cp tests/v1/kv_offload/pd_connector_test_prompt.py <pod_name>:/tmp/pd_connector_test_prompt.py
   ```

6. **Deploy**: Run the deploy script using the config resolved in step 2. Environment variable overrides take precedence over the config file (deploy.sh already handles this), so pass preset values as env vars when needed. If `--debug` was specified, prepend `LOG_LEVEL=DEBUG` to the deploy command.

7. **Sanity test**: After deploy completes, run a two-tier verification:

   **Tier 1 — Output correctness (always):**

   Send a request through the proxy using a prompt long enough to span 2 full KV blocks (≥32 tokens for block_size=16) and validate the response:
   ```
   oc exec <pod_name> -- curl -sf http://127.0.0.1:8192/v1/completions \
     -H 'Content-Type: application/json' \
     -d '{"model":"<model_from_config>","prompt":"Explain in detail the history of the French Revolution, starting from the social and economic conditions in France during the late 18th century. Discuss the role of Enlightenment thinkers such as Voltaire, Rousseau, and Montesquieu in shaping revolutionary ideals. Describe the events leading up to the storming of the Bastille on July 14, 1789, including the Estates-General, the Tennis Court Oath, and the growing unrest among the Third Estate. Analyze the Declaration of the Rights of Man and of the Citizen and its significance in establishing principles of liberty, equality, and fraternity. Examine the radical phase of the Revolution, including the Reign of Terror under Robespierre and the Committee of Public Safety. Discuss the fall of the monarchy, the execution of King Louis XVI, and the establishment of the First French Republic. Explain how internal conflicts between the Girondins and the Jacobins shaped the political landscape. Describe the Thermidorian Reaction and the establishment of the Directory as a moderate government. Finally, discuss the rise of Napoleon Bonaparte and how the Revolution fundamentally transformed French society, restructured systems of governance, and influenced democratic movements across Europe and the Americas throughout the nineteenth century. In summary, the key lesson of the Revolution is","max_tokens":20}'
   ```
   Verify:
   - Response is valid JSON
   - `.choices[0].text` is non-empty
   - `.usage.prompt_tokens >= 32` (confirms 2+ full blocks)
   - `.usage.completion_tokens > 0`

   If tier 1 fails, report the failure and show log tails (step 8).

   **Tier 2 — KV transfer verification (only when `--debug` was specified):**

   *Single-pod only.* The KV-transfer test script snapshots both engine logs locally and looks for new lines after sending a request, which assumes both logs live on the same pod. In two-pod mode the decoder log is on `<decoder_pod>`, so use the metrics-based verification below instead.

   **Single-pod:** run the KV transfer test script on the pod:
   ```
   oc exec <pod_name> -- python3 /tmp/pd_connector_test_prompt.py \
     --proxy http://127.0.0.1:8192 \
     --model "<model_from_config>" \
     --max-tokens 50 \
     --prefiller-log /tmp/prefiller.log \
     --decoder-log /tmp/decoder.log
   ```
   This script:
   - Uses a randomized prompt prefix to bust prefix-cache hits (ensures fresh KV transfer)
   - Snapshots log sizes before the request
   - Sends a completion request and checks new log lines for KV transfer indicators
   - Reports PASS if all NIXL transfers completed and were acknowledged by the decoder
   - Exits with code 1 on FAIL

   **Two-pod (metrics-based check):** read `vllm:kv_offload_total_bytes_total` from each engine before and after a long-prompt request through the proxy. PASS if `prefiller{transfer_type="GPU_to_CPU"}` and `decoder{transfer_type="CPU_to_GPU"}` both grew by the same nonzero amount (KV pushed by prefiller equals KV pulled by decoder). Sketch:
   ```
   PREF=10.x.x.x; DEC=10.y.y.y    # PREFILLER_ADDR / DECODER_ADDR from /tmp/deploy_state.env
   metric() { oc exec <pod_name> -- curl -s "http://$1:$2/metrics" | grep "kv_offload_total_bytes_total" | grep "$3"; }
   metric $PREF 8100 GPU_to_CPU; metric $DEC 8200 CPU_to_GPU      # before
   # send a long-prompt curl through the proxy (tier 1)
   metric $PREF 8100 GPU_to_CPU; metric $DEC 8200 CPU_to_GPU      # after — both deltas should be equal and nonzero
   ```

   If `--debug` was NOT specified, print:
   ```
   KV transfer verification skipped (no --debug flag). Re-deploy with --debug to verify KV block transfers.
   ```

8. **Report**: Show the deploy summary and test results:
   - Output test: PASS or FAIL
   - KV transfer: PASS (with block counts) / FAIL / skipped (INFO log level)

   If anything fails, show the relevant log tails. Note that in two-pod mode the decoder log lives on `<decoder_pod>`, not `<pod_name>`:
   ```
   oc exec <pod_name>     -- tail -30 /tmp/prefiller.log
   oc exec <decoder_pod>  -- tail -30 /tmp/decoder.log     # single-pod: <pod_name>
   oc exec <pod_name>     -- tail -30 /tmp/proxy.log
   ```
