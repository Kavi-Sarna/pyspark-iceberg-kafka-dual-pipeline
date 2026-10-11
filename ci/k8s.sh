#!/usr/bin/env bash
# Validate the manifests against the published schemas.
#
# Not part of ci/run.sh. It fetches the Kubernetes and operator schemas over
# the network, so an offline machine or a moved URL would fail a run for a
# reason unrelated to the commit. tests/unit/test_manifests.py holds the
# invariants that matter without a network, and runs in ci/run.sh.
#
# What this adds is the schemas themselves: field names, types and required
# keys, as the API server and the operator define them. A CRD prunes unknown
# fields rather than rejecting them, so a typo is otherwise invisible until
# the setting it was meant to apply turns out not to have applied.
set -euo pipefail

cd "$(dirname "$0")/.."

K8S_VERSION="${K8S_VERSION:-v1.35.0}"
OPERATOR_VERSION="${OPERATOR_VERSION:-v2.5.2}"

# -----------------------------------------------------------------------------
# Phase 1: Schema & Structural Validation (Runs Offline, No Cluster)
# -----------------------------------------------------------------------------
uv run --with jsonschema --with pyyaml python - "$K8S_VERSION" "$OPERATOR_VERSION" <<'PY'
import json
import pathlib
import sys
import urllib.request

import yaml
from jsonschema import Draft7Validator

k8s_version, operator_version = sys.argv[1], sys.argv[2]

BUILTIN = (
    "https://raw.githubusercontent.com/yannh/kubernetes-json-schema/master/"
    f"{k8s_version}-standalone-strict/{{}}.json"
)
CRD = (
    "https://raw.githubusercontent.com/kubeflow/spark-operator/"
    f"{operator_version}/config/crd/bases/sparkoperator.k8s.io_{{}}.yaml"
)
CUSTOM = {
    "SparkApplication": "sparkapplications",
    "ScheduledSparkApplication": "scheduledsparkapplications",
}

cache: dict[str, object] = {}


def builtin_schema(kind: str, api_version: str):
    group = api_version.split("/")[0] if "/" in api_version else ""
    version = api_version.split("/")[-1]
    name = f"{kind}-{group.split('.')[0]}-{version}" if group else f"{kind}-{version}"
    name = name.lower()
    if name not in cache:
        cache[name] = json.load(urllib.request.urlopen(BUILTIN.format(name)))
    return cache[name]


def crd_schema(kind: str):
    if kind not in cache:
        raw = urllib.request.urlopen(CRD.format(CUSTOM[kind])).read()
        crd = yaml.safe_load(raw)
        cache[kind] = crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]
    return cache[kind]


def unknown_fields(node, schema, path):
    """A structural schema drops what it does not recognise, so an unknown key
    is not an error anywhere - it is a setting that silently did nothing."""
    found = []
    if not isinstance(node, dict) or not isinstance(schema, dict):
        return found
    if schema.get("x-kubernetes-preserve-unknown-fields"):
        return found
    properties = schema.get("properties")
    if properties is None:
        extra = schema.get("additionalProperties")
        if isinstance(extra, dict):
            for key, value in node.items():
                found += unknown_fields(value, extra, f"{path}.{key}")
        return found
    for key, value in node.items():
        if key not in properties:
            found.append(f"{path}.{key}")
            continue
        child = properties[key]
        if isinstance(value, dict):
            found += unknown_fields(value, child, f"{path}.{key}")
        elif isinstance(value, list) and child.get("type") == "array":
            for index, item in enumerate(value):
                found += unknown_fields(item, child.get("items", {}), f"{path}.{key}[{index}]")
    return found


failures = 0
checked = 0

for path in sorted(pathlib.Path("k8s").glob("*.yaml")):
    if path.name == "kind.yaml":
        # kind publishes no JSON schema for its cluster config.
        yaml.safe_load(path.read_text())
        print(f"{path.name:20} parsed")
        continue
    
    for doc in yaml.safe_load_all(path.read_text()):
        if not doc:
            continue
        kind = doc["kind"]
        checked += 1
        label = f"{path.name:20} {kind}/{doc['metadata']['name']}"

        if kind in CUSTOM:
            schema = crd_schema(kind)
            errors = sorted(Draft7Validator(schema).iter_errors(doc), key=lambda e: list(e.path))
            extra = unknown_fields(doc.get("spec", {}), schema["properties"]["spec"], "spec")
        else:
            schema = builtin_schema(kind, doc["apiVersion"])
            errors = sorted(Draft7Validator(schema).iter_errors(doc), key=lambda e: list(e.path))
            extra = []
        
        if errors or extra:
            failures += 1
            print(f"{label}  FAIL")
            for error in errors[:6]:
                print("     ", list(error.path), error.message[:200])
            for field in extra[:10]:
                print("      unknown field:", field)
        else:
            print(f"{label}  ok")

print(f"\n{checked} resources, {failures} failed")
sys.exit(1 if failures else 0)
PY

# -----------------------------------------------------------------------------
# Phase 2: Live Cluster Verification (Runs Only When Explicitly Requested)
# -----------------------------------------------------------------------------
if [[ "${1:-}" != "--cluster" && "${RUN_CLUSTER:-0}" != "1" ]]; then
  exit 0
fi

echo ""
echo "==> Schemas validated. Starting Kind cluster verification..."

# Check prerequisites
for bin in docker kind kubectl helm; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    echo "ERROR: missing required CLI tool: $bin" >&2
    echo "       Install it before running the cluster suite (e.g. 'brew install $bin')." >&2
    exit 1
  fi
done

if ! docker info >/dev/null 2>&1; then
  echo "ERROR: no Docker daemon reachable; start Docker Desktop first." >&2
  exit 1
fi

cleanup() {
  local exit_code=$?
  if [ $exit_code -ne 0 ]; then
    echo "==> Failure detected (exit code $exit_code). Dumping pod diagnostics..."
    kubectl -n pipeline get pods -o wide || true
    echo ""
    echo "--- Kafka Pod Description ---"
    kubectl -n pipeline describe pod -l app=kafka || true
    echo ""
    echo "--- Kafka Logs (tail 50) ---"
    kubectl -n pipeline logs -l app=kafka --tail=50 || true
    echo ""
    echo "--- Schema Registry Logs (Current) ---"
    kubectl -n pipeline logs -l app=schema-registry --tail=50 || true
    echo "--- Schema Registry Logs (Previous Container) ---"
    kubectl -n pipeline logs -l app=schema-registry --previous --tail=50 || true
    echo ""
    echo "--- Spark Operator Logs (tail 50) ---"
    kubectl -n spark-operator logs -l app.kubernetes.io/name=spark-operator --tail=50 || true
  fi
  echo "==> Cleaning up Kind cluster..."
  make kind-down >/dev/null 2>&1 || true
}
trap cleanup EXIT

# 1. Build local container and create the kind cluster
make image
make kind-up

# 2. Deploy data stack and wait for bootstrap job completion
make k8s-up

# 3. Submit scheduled batch job and assert scheduleState
make k8s-batch
kubectl -n pipeline wait --for=jsonpath='{.status.scheduleState}'=Scheduled scheduledsparkapplication/pipeline-batch --timeout=180s
echo "==> Schedule registered: $(kubectl -n pipeline get scheduledsparkapplication pipeline-batch -o jsonpath='{.status.scheduleState}')"

echo "==> Kind cluster verification passed."
