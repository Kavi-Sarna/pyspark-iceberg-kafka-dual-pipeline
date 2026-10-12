#!/usr/bin/env bash
# Runs on every codespace start, including a resume after the 30-minute idle
# stop. Brings the cluster up so the forwarded UIs work as soon as you look.
#
# Never fails the start: a broken cluster should leave you with a working editor
# to fix it in, not a codespace that will not open.
set -euo pipefail

cd "$(dirname "$0")/.."

if ! make up; then
  echo
  echo "WARNING: the cluster did not come up. The codespace is still usable." >&2
  echo "         Run 'make up' to see the error, or 'make smoke' end to end." >&2
  exit 0
fi

cat <<'EOF'

Cluster is up. Open the Ports panel, or:

  make submit   submit the app and watch it on the master UI
  make smoke    the whole cycle with assertions, then teardown
  make down     stop it and reclaim the memory

To show the streaming job doing real work, in two terminals:

  make stream AWAIT_SECONDS=900          # terminal 1: the query
  make load SHIPMENTS=100000             # terminal 2: the stream

Then watch Grafana on port 3000, dashboard "Streaming join", and the driver
UI on 4040.

To show someone: set a port to Public in the Ports panel. Nothing here has
authentication - Grafana runs with anonymous admin - so make a port public
only for as long as you need it, and stop the codespace afterwards.
EOF
