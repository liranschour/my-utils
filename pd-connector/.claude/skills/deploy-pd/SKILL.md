---
name: deploy-pd
description: This skill should be used when the user asks to "deploy pd", "deploy pd connector", "deploy prefill-decode", "run pd sanity test", "deploy vllm on pod", or needs to deploy a PD-disaggregated vLLM environment on an OC pod and verify it works.
---

# Deploy PD Connector on OC Pod

Deploy a PD-disaggregated vLLM environment (prefiller + decoder + proxy) on an OC pod and verify it works.

## Arguments

- `$ARGUMENTS` — `<pod_name> [--large] [--debug]`
  - `pod_name` (required) — the OC pod name (e.g. `llmd-decoder`, `llmd-transport-decoder`)
  - `--large` (optional) — use the large model preset instead of the default small one
  - `--debug` (optional) — deploy with `LOG_LEVEL=DEBUG` for verbose vllm logging to help diagnose failures

## Model Presets

| Preset | Model | GPU Mem | Max Len | Block Size | CPU Bytes |
|--------|-------|---------|---------|------------|-----------|
| **small** (default) | Qwen/Qwen3-0.6B | 0.12 | 512 | 128 | 2147483648 (2GB) |
| **large** (`--large`) | Qwen/Qwen3-8B | 0.90 | 8192 | 16 | 4294967296 (4GB) |

## Steps

1. **Parse arguments**: Extract `pod_name` and check for `--large` and `--debug` flags from `$ARGUMENTS`.

2. **Resolve config**: Based on the preset:
   - **small** (default): Look for `tests/v1/kv_offload/configs/cluster_pd_<pod_name>.env` (replacing dashes with underscores). If it doesn't exist, use env var overrides with the small preset values.
   - **large** (`--large`): Override model parameters via environment variables using the large preset values above, with the pod name set to `<pod_name>`:
     ```
     PREFILLER_POD=<pod_name> DECODER_POD=<pod_name> PROXY_POD=<pod_name> \
     MODEL=Qwen/Qwen3-8B GPU_MEM_UTIL=0.90 MAX_MODEL_LEN=8192 BLOCK_SIZE=16 \
     CPU_BYTES=4294967296 VLLM_BIN=vllm PYTHON_BIN=python3 \
     bash tests/v1/kv_offload/deploy.sh --config tests/v1/kv_offload/configs/cluster_pd.env
     ```

2. **Pre-flight checks** (run these in parallel):
   - Verify the pod is running: `oc get pod <pod_name>`
   - Verify GPUs are available: `oc exec <pod_name> -- nvidia-smi --query-gpu=memory.free --format=csv,noheader`
   - Verify vllm is installed: `oc exec <pod_name> -- python3 -c "import vllm; print(vllm.__version__)"`

3. **Sync vllm source code**: Ensure the pod's vllm source at `/vllm-workspace/vllm` matches the local working directory.

   a. **Check branch**: Verify the pod is on the same branch as local:
      ```
      oc exec <pod_name> -- bash -c "cd /vllm-workspace/vllm && git branch --show-current"
      ```
      Compare with the local branch. If different, fetch and checkout:
      ```
      oc exec <pod_name> -- bash -c "cd /vllm-workspace/vllm && git fetch origin <branch> && git checkout <branch>"
      ```

   b. **Check commits**: Compare local HEAD with the pod's HEAD:
      ```
      # local
      git rev-parse HEAD
      # pod
      oc exec <pod_name> -- bash -c "cd /vllm-workspace/vllm && git rev-parse HEAD"
      ```
      If they differ, pull on the pod:
      ```
      oc exec <pod_name> -- bash -c "cd /vllm-workspace/vllm && git pull origin <branch>"
      ```

   c. **Sync uncommitted changes**: Check for locally modified (dirty) files:
      ```
      git diff --name-only HEAD
      ```
      For each modified file, copy it to the pod at the matching path under `/vllm-workspace/vllm/`:
      ```
      oc cp <local_file> <pod_name>:/vllm-workspace/vllm/<relative_path>
      ```
      Report which files were synced.

4. **Copy proxy script** to the pod:
   ```
   oc cp tests/v1/kv_offload/pd_connector_proxy.py <pod_name>:/tmp/pd_connector_proxy.py
   ```

5. **Deploy**: Run the deploy script using the config resolved in step 2. Environment variable overrides take precedence over the config file (deploy.sh already handles this), so pass preset values as env vars when needed. If `--debug` was specified, prepend `LOG_LEVEL=DEBUG` to the deploy command.

6. **Sanity test**: After deploy completes, send a test request through the proxy:
   ```
   oc exec <pod_name> -- curl -sf http://127.0.0.1:8192/v1/completions \
     -H 'Content-Type: application/json' \
     -d '{"model":"<model_from_config>","prompt":"Hello, my name is","max_tokens":20}'
   ```

7. **Report**: Show the deploy summary and test result. If anything fails, show the relevant log tail:
   ```
   oc exec <pod_name> -- tail -30 /tmp/prefiller.log
   oc exec <pod_name> -- tail -30 /tmp/decoder.log
   oc exec <pod_name> -- tail -30 /tmp/proxy.log
   ```
