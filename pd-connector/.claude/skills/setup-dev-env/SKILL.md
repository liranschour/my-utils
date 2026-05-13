---
name: setup-dev-env
description: This skill should be used when the user asks to "setup dev env", "install vllm on pod", "bootstrap pod", or needs to install a vLLM development environment from source on a new OC pod.
---

# Setup Dev Environment on OC Pod

Install uv, clone the vLLM fork, install from source in editable mode, and verify it works.

## Arguments

- `$ARGUMENTS` — `<pod_name>` (required) — the OC pod name (e.g. `llmd-decoder`)

## Steps

1. **Parse arguments**: Extract `pod_name` from `$ARGUMENTS`.

2. **Pre-flight check**: Verify the pod is running:
   ```
   oc get pod <pod_name>
   ```

3. **Install git** (not present on base image):
   ```
   oc exec <pod_name> -- bash -c "apt-get update && apt-get install -y git"
   ```

4. **Install uv** (if not already present):
   ```
   oc exec <pod_name> -- bash -c "which uv || (curl -LsSf https://astral.sh/uv/install.sh | sh)"
   ```

5. **Clone or update the repo** at `/vllm-workspace/vllm`:
   - If `/vllm-workspace/vllm/.git` exists, fetch and checkout:
     ```
     oc exec <pod_name> -- bash -c "cd /vllm-workspace/vllm && git fetch origin pd-connector-new && git checkout pd-connector-new && git pull origin pd-connector-new"
     ```
   - Otherwise, clone fresh:
     ```
     oc exec <pod_name> -- bash -c "cd /vllm-workspace && git clone -b pd-connector-new https://github.com/liranschour/vllm.git vllm"
     ```

6. **Install vllm in editable mode** using uv (with cu130 variant to match the pod's CUDA 13 runtime):
   ```
   oc exec <pod_name> -- bash -c "cd /vllm-workspace/vllm && VLLM_USE_PRECOMPILED=1 VLLM_PRECOMPILED_WHEEL_VARIANT=cu130 uv pip install -e . --torch-backend=auto --system"
   ```

   Then install CUDA 12 runtime (needed by nixl_ep_cpp which is pre-installed in the docker image against CUDA 12):
   ```
   oc exec <pod_name> -- bash -c "uv pip install nvidia-cuda-runtime-cu12 --system"
   ```

7. **Sanity test** — verify vllm imports, CLI works, and GPUs are visible:
   ```
   oc exec <pod_name> -- python3 -c "
   import vllm
   import torch
   print('vllm imported OK')
   print(f'CUDA available: {torch.cuda.is_available()}')
   print(f'GPUs: {torch.cuda.device_count()}')
   "
   oc exec <pod_name> -- vllm --version
   ```

8. **Report**: Show success or failure. If the install step fails, show the last 30 lines of output for debugging.
