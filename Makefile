SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

# ---------------------------------------------------------------------------------------
# Environment
# 
# The shell is part of the build. Pin what the job depends on here so it behaves 
# the same on our laptop, a colleague's, and in CI - rather than inheriting
# whatever happens to be exported.
# ---------------------------------------------------------------------------------------

# The pyspark wheel ships its own Spark. A stale SPARK_HOME makes the JVM load
# a different distribution and fail without ever mentioning SPARK_HOME.
unexport SPARK_HOME

# On macOS, resolve a JDK through Apple's registry rather than trusting
# JAVA_HOME, which can point at a Homebrew formula that has since been removed.
# CI sets JAVA_HOME itself via setup-java, so leave it alone there.
ifeq ($(shell uname -s),Darwin)
  DETECTED_JAVA_HOME := $(shell /usr/libexec/java_home -v 21 2>/dev/null || \
  								/usr/libexec/java_home -v 17 2>/dev/null)
  ifneq ($(DETECTED_JAVA_HOME),)
    export JAVA_HOME := $(DETECTED_JAVA_HOME)
  endif
endif

# Spark launches Python workers with whatever python3 is on PATH. Pin them to
# this project's interpreter so driver and workers cannot disagree.
export PYSPARK_PYTHON := $(CURDIR)/.venv/bin/python
export PYSPARK_DRIVER_PYTHON := $(CURDIR)/.venv/bin/python

# The driver binds to whatever the hostname resolves to, which on a laptop can 
# be a stale DHCP lease no interface holds any more. Loopback is always right 
# for local and unit-test runs; a cluster submit sets its own bind address.
export SPARK_LOCAL_IP := 127.0.0.1

.PHONY: help env sync fmt lint types test ci run image image-run scan clean

help: ## Show this help
	@grep -hE '^[a-z-]+:.*?##' $(MAKEFILE_LIST) \
	  awk -F':.*?## ' '{printf "  \033[36m%-8s\033[0m %s\n", $$1, $$2}'

env: ## Show and validate the resolved environment
	@echo "JAVA_HOME                = $${JAVA_HOME:-(unset)}"
	@echo "SPARK_HOME               = $${SPARK_HOME:-(unset)}   <- must be unset"
	@echo "PYSPARK_PYTHON           = $(PYSPARK_PYTHON)"
	@echo "PYSPARK_DRIVER_PYTHON    = $(PYSPARK_DRIVER_PYTHON)"
	@test -n "$${JAVA_HOME:-}" \
	  || { echo "ERROR: no JAVA_HOME. Install a JDK 17 or 21."; exit 1; }
	@test -x "$${JAVA_HOME}/bin/java" \
	  || { echo "ERROR: JAVA_HOME points at a path that does not exists: $${JAVA_HOME}"; exit 1; }
	@"$${JAVA_HOME}/bin/java" -version 2>&1 | head -1

sync: ## Install dependencies from the lockfile
	uv sync

fmt: ## Format the code
	uv run ruff format .

lint: ## Lint (undefined names, unused imports, dead code)
	uv run ruff check .

types: ## Type-check
	uv run mypy

test: env ## Run the unit tests
	uv run pytest -q

ci: ## Run everything CI runs
	./ci/run.sh

run: env ## Run the app
	uv run pipeline

image: ## Build the container image
	docker build -f docker/Dockerfile -t pipeline:local .

image-run: image ## Run the app inside the container
	docker run --rm pipeline:local

up: ## Start the local Spark cluster
	docker compose -f docker/compose.yaml up --detach --build --wait \
	  spark-master spark-worker spark-history
	@echo "master   http://localhost:8080"
	@echo "worker   http://localhost:8081"
	@echo "history  http://localhost:18080"

down: ## Stop the cluster and drop its volumes
	docker compose -f docker/compose.yaml down --volumes --remove-orphans

submit: up ## Submit the app to the running cluster
	docker compose -f docker/compose.yaml run --rm submit

smoke: ## Bring up a cluster, submit, assert, tear down
	./ci/smoke.sh

scan: ## Scan the image for fixable HIGH/CRITICAL vulnerabilities
	./ci/scan.sh

clean: ## Remove caches and local Spark artifacts
	rm -rf .pytest_cache .mypy_cache .ruff_cache .trivy spark-warehouse metastore_db derby.log
	find . -name __pycache__ -type d -not -path './.venv/*' -exec rm -rf {} +
