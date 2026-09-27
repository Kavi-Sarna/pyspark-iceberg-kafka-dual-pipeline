# syntax=docker/dockerfile:1

ARG SPARK_IMAGE=apache/spark:4.0.4-python3

# --------------------------------------------------------------------------------
# Stage 1 - resolve the environment with uv.
# 
# Same base as the runtime stage: one image to track for CVEs, one to bump, and
# the venv is built against the glibc it will run on.
# --------------------------------------------------------------------------------
FROM ${SPARK_IMAGE} AS builder

USER root

COPY --from=ghcr.io/astral-sh/uv:0.10.6 /uv /usr/local/bin/uv

# uv fetches the interpreter over HTTPS and fails without a CA bundle.
RUN set -eux; \
    if [ ! -s /etc/ssl/certs/ca-certificates.crt ]; then \
        apt-get update; \
        apt-get install -y --no-install-recommends ca-certificates; \
        rm -rf /var/lib/apt/lists/*; \
    fi

# only-managed: the base image has Python 3.10 on PATH and this project pins 
# 3.12. UV_PYTHON_INSTALL_DIR is copied verbatim into the runtime stage, so the 
# absolute path the venv records in pyvenv.cfg stays valid.
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_INSTALL_DIR=/python \
    UV_PYTHON_PREFERENCE=only-managed

RUN uv python install 3.12

WORKDIR /app

# Dependencied before source: so editing code does not re-resolve PySpark.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project --no-editable

# --no-editable copies the package into site-packages rather than a .pth pointing at 
# /app/src, so the venv alone is enough at runtime.
COPY README.md ./
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

# --------------------------------------------------------------------------------
# Stage 2 - the Spark runtime. Nothing from the builder's package manager, 
# apt lists or uv cache reaches this stage.
# --------------------------------------------------------------------------------
FROM ${SPARK_IMAGE}

USER root

COPY --from=builder /python /python
COPY --from=builder /app/.venv /app/.venv

# SPARK_HOME=/opt/spark is set by the base image and pyspark prefers it over the 
# distribution inside its own wheel, so the image's jars are what run. Fail the 
# build on skew rather than at the first action.
RUN set -eux; \
    v="$(/app/.venv/bin/python -c 'import pyspark; print(pyspark.__version__)')"; \
    echo "wheel=${v}  distribution=$(ls /opt/spark/jars/spark-core_*.jar)"; \
    ls /opt/spark/jars/spark-core_*-"${v}".jar > /dev/null

# PYSPARK_PYTHON: workers otherwise launch the image's 3.10 and fail with
# PYTHON_VERSION_MISMATCH. HOME: uid 185's home is /nonexistent.
ENV PATH="/app/.venv/bin:${PATH}" \
    PYSPARK_PYTHON=/app/.venv/bin/python \
    PYSPARK_DRIVER_PYTHON=/app/.venv/bin/python \
    HOME=/tmp

# A named volume mounted here inherits this ownership; created fresh by Docker
# it would be root-owned and uid 185 could not write event logs.
RUN mkdir -p /tmp/spark-events && chown 185:185 /tmp/spark-events

# Numeric, so a pod with runAsNonRoot can verify it.
USER 185
WORKDIR /app

# The base entrypoint dispatches `driver` and `executor` for Spark on 
# Kubernetes; replacing it would work here and break executor pods on a cluster.
ENTRYPOINT ["/opt/entrypoint.sh"]
CMD ["/app/.venv/bin/python", "-m", "pipeline.main"]
