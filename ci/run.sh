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

# Fails loudly if uv.lock has drifted from pyproject.toml. This is the 
# reproducibility gate: CI must install exactly what we installed.
uv sync --frozen

# Cheapest checks first, so a typo fails in seconds rather than minutes.
uv run ruff format --check .
uv run ruff check .
uv run mypy
uv run pytest -q

# PR 3 add:   docker build -f docker/Dockerfile -t pipeline:local .
