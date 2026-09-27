#!/usr/bin/env bash
# Runs once, when the codespace is created. The slow part: install uv, resolve 
# the locked dependencies, and build the image the cluster runs.
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v uv >/dev/null 2>&1; then
    pipx install uv || python3 -m pip install --user uv
fi
export PATH="${HOME}/.local/bin:${PATH}"

uv sync --frozen

# Built here rather than on every start, so a resume is quick.
make image

echo
echo "Ready. The cluster starts on every codespace start; see post-start.sh."
