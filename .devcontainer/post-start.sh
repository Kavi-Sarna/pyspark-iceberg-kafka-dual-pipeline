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

    make submit     submit the app and watch it on the master UI
    make smoke      the whole cycle with assertions, then teardown
    make down       stop it and reclain the memory

To show someone: set a port to Public in the Ports panel. The Spark UIs have 
no authentication, so make it public only for as long as you need it.
EOF
