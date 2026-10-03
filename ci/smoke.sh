#!/usr/bin/env bash
# End-to-end proof that the app runs on a real cluster, against an empty stack.
# 
# This is the check behind "main is always runnable". Unit tests prove the code;
# this proves the artifact, the cluster and the submit path agree. It is 
# deterministic - unlike the vulnerability scan - so it belongs in ci/run.sh.
set -euo pipefail

cd "$(dirname "$0")/.."

COMPOSE=(docker compose -f docker/compose.yaml)
MASTER_URL="spark://spark-master:7077"

if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
  echo "ERROR: no Docker daemon reachable; cannot run the smoke test." >&2
  exit 1
fi

cleanup() { "${COMPOSE[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

cleanup         # a stack left over from a failed run would mask a real failure

# --wait blocks until every healthcheck passes, so the submit below cannot race
# a master that is still binding its port.
# Spark only: the smoke test does not touch the catalog yet, and starting 
# object storage for it would just make it slower.
# Build once, here, rather than letting compose rebuild per service: each 
# rebuild re-resolves the base images and anonymous registries rate-limit.
docker build -f docker/Dockerfile -t pipeline:local .
"${COMPOSE[@]}" up --detach --build --wait spark-master spark-worker spark-history

log="$(mktemp)"
if ! "${COMPOSE[@]}" run --rm submit >"${log}" 2>&1; then
  cat "${log}" >&2
  echo "ERROR: spark-submit exited non-zero." >&2
  exit 1
fi
cat "${log}"

# The job must report the cluster master. Without this assertion a regression
# that falls back to local[*] still exits 0, and the smoke test passes while 
# never touching the cluster at all.
if ! grep -q "master ${MASTER_URL}" "${log}"; then
  echo "ERROR: expected 'master ${MASTER_URL}' in the output." >&2
  echo "       The job ran, but not on the cluster." >&2
  exit 1
fi

# And the master must agree that an application finished, rather than the
# driver having reported success to itself.
python3 - "$MASTER_URL" <<'PY'
import json, sys, urllib.request

state = json.load(urllib.request.urlopen("http://localhost:8080/json/", timeout=10))
done = state.get("completedapps", [])
names = [a.get("name") for a in done]
print(f"master reports {len(done)} completed application(s): {names}")
if not any(n == "pipeline-smoke" for n in names):
  sys.exit("ERROR: master has no completed applications named 'pipeline-smoke'.")
PY

echo
echo "SMOKE OK -  the app ran on the cluster and the master recorded it."
