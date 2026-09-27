#!/usr/bin/env bash
# Image vulnerability scan.
# 
# Not part of ci/run.sh. Its result depends on a vulnerability database that 
# changes daily, not on the commit, so it must not gate a PR: an unrelated CVE 
# published overnight would block work nobody can fix from this repo. It runs on 
# a schedule against main instead, and on demand via `make scan`.
set -euo pipefail

cd "$(dirname "$0")/.."

IMAGE="${IMAGE:-pipeline:local}"
TRIVY_IMAGE="${TRIVY_IMAGE:-aquasec/trivy:0.74.0}"
SEVERITY="${SEVERITY:-HIGH,CRITICAL}"

# 0 reports and succeeds, 1 fails the run. The nightly workflow sets 1 so a new 
# finding surfaces as a failed run; locally you want to read the list, not be 
# stopped by it.
SCAN_EXIT_CODE="${SCAN_EXIT_CODE:-0}"

if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
    echo "ERROR: no Docker daemon reachable; cannot scan." >&2
    exit 1
fi

mkdir -p .trivy/db

docker build -f docker/Dockerfile -t "${IMAGE}" .

# Hand trivy a tar rather than mounting /var/run/docker.sock. The socket would 
# give the scanner root-equivalent control of the host daemon just to read 
# layers.
docker save "${IMAGE}" -o .trivy/image.tar
trap 'rm -f .trivy/image.tar' EXIT

# --ignore-unfixed: findings with no fixed version cannot be resolved by 
# upgrading, only by changing base or accepting them. Listing them beside 
# actionable ones is how a report gets ignored.
docker run --rm -v "${PWD}/.trivy:/t" "${TRIVY_IMAGE}" image \
    --cache-dir /t/db \
    --input /t/image.tar \
    --scanners vuln \
    --severity "${SEVERITY}" \
    --ignore-unfixed \
    --exit-code "${SCAN_EXIT_CODE}" \
    | tee .trivy/report.txt
