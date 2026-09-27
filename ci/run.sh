#!/usr/bin/env bash
# The pipeline contract. This is what "green" means, locally and in CI.
# 
# Every provider - Github Actions, Jenkins, Buildkite - should call this script 
# and nothing else. If a check exists only in a providers's YAML we cannot run 
# it on our laptop, and we end up debugging through commit-push-wait cycles.
set -euo pipefail

cd "$(dirname "$0")/.."

# The pyspark wheel ships its own Spark distribution, A stale SPARK_HOME makes
# the JVM load a different one and fail in ways that never mention SPARK_HOME.
unset SPARK_HOME

# The driver otherwise binds to whatever the hostname resolves to, which can be 
# an address the machine no longer holds. Unit tests run in local mode only.
export SPARK_LOCAL_IP=127.0.0.1

# Fails loudly if uv.lock has drifted from pyproject.toml. This is the 
# reproducibility gate: CI must install exactly what we installed.
uv sync --frozen

# Cheapest checks first, so a typo fails in seconds rather than minutes.
uv run ruff format --check .
uv run ruff check .
uv run mypy
uv run pytest -q

# ------ image -------------------------------------------------------------
# The image is part of the contract. CI sets REQUIRE_DOCKER=1 so a missing
# daemon is a hard failure there. Locally the step is skipped with a 
# load warning, so `make ci` still works on a machine where Docker is not 
# running - an unbuildable image must not make main unrunnable.
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    docker build -f docker/Dockerfile -t pipeline:local .

    # The image's own config is the contract on Kubernetes: a named user cannot
    # satisfy runAsNonRoot, and replacing the base entrypoint breaks executor pods.
    user=$(docker inspect --format '{{.Config.User}}' pipeline:local)
    entry=$(docker inspect --format '{{json .Config.Entrypoint}}' pipeline:local)
    [[ "${user}" == "185" ]] \
        || { echo "ERROR: image runs as '${user}', expected 185." >&2; exit 1; }
    [[ "${entry}" == '["/opt/entrypoint.sh"]' ]] \
        || { echo "ERROR: entrypoint is ${entry}, expected /opt/entryppoint.sh." >&2; exit 1; }
elif [[ "${REQUIRE_DOCKER:-0}" == "1" ]]; then
    echo "ERROR: REQUIRE_DOCKER=1 but no Docker daemon is reachable." >&2
    exit 1
else
    echo
    echo "WARNING: skipped 'docker build' - no Docker daemon reachable."
    echo "         CI runs this step. Start Docker and re-run to check it here."
    echo
fi
