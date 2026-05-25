---
name: worktree-test
description: This skill should be used when the user asks to "run tests in worktree", "setup worktree for testing", "symlink so files", or needs to run vllm unit tests from a git worktree that lacks compiled C extensions.
---

# Run Unit Tests in a Git Worktree

Symlink compiled `.so` extensions from the main repo into the current worktree so that pytest can import vllm without rebuilding from source.

## Prerequisites

- Current working directory must be a git worktree of the vllm repo
- The main repo at `/home/lirans/vllm` must have a working editable install with compiled extensions
- The venv is at `/home/lirans/vllm/.venv`

## Steps

1. **Symlink compiled extensions** from the main repo into the worktree:
   ```bash
   find /home/lirans/vllm/vllm -name "*.so" -exec bash -c '
     for src; do
       rel="${src#/home/lirans/vllm/}"
       dst="<worktree_path>/$rel"
       mkdir -p "$(dirname "$dst")"
       ln -sf "$src" "$dst"
     done
   ' _ {} +
   ```

2. **Verify** vllm imports correctly from the worktree:
   ```bash
   /home/lirans/vllm/.venv/bin/python -c "import vllm; print(vllm.__file__)"
   ```
   This should print a path inside the worktree.

3. **Run the requested tests** using the venv python:
   ```bash
   /home/lirans/vllm/.venv/bin/python -m pytest <test_path> -x -v
   ```

## Notes

- The worktree's local `vllm/` directory shadows the editable install because pytest adds CWD to `sys.path`. Symlinking the `.so` files makes the worktree's copy functional.
- The symlinks are not tracked by git (`.so` files are in `.gitignore`).
- If new C extensions are compiled in the main repo, re-run step 1.
