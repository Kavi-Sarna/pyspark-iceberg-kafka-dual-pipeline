# PR 2 — walkthrough

**Goal:** one command that means "green", runnable identically on your laptop and in CI — and a `Makefile` that stops the job depending on how your shell happens to be configured.

**Why now, before any logic:** the first PR that goes green should be the one that proves CI works. Add the pipeline first and you discover your CI is broken while also debugging a join.

**Not in this PR:** Dockerfile (PR 3), Compose and `make smoke` (PR 4).

---

## Step 0 — the problem this PR solves

PR 1 ran only because you fixed your shell by hand. Three separate environment faults showed up while building it, none of them visible in the source:

| Fault                                                       | Symptom                                                           |
| ----------------------------------------------------------- | ----------------------------------------------------------------- |
| `JAVA_HOME` pointing at a removed Homebrew formula          | `Java gateway process exited before sending its port number`      |
| A stale `SPARK_HOME` from an old Spark install              | Same gateway error, or `'JavaPackage' object is not callable`     |
| Spark workers launching a different Python than the driver  | `PYTHON_VERSION_MISMATCH: Python in worker has different version` |


Every one of them breaks the app completely, none of them mentions the real cause, and none is fixed by anything in `src/`. **Anything that determines whether your program runs is part of the build**, so it belongs in the repo.


## Step 1 — `ci/run.sh`

A CI pipeline is just an agreed list of commands that must exit 0. Write that list as a shell script the repo owns, and the provider becomes a thin wrapper.

```bash
mkdir -p ci
```

`ci/run.sh`:

```bash
#!/usr/bin/env bash
# The pipeline contract. This is what "green" means, locally and in CI.
#
# Every provider — GitHub Actions, Jenkins, Buildkite — should call this script
# and nothing else. If a check exists only in a provider's YAML you cannot run
# it on your laptop, and you end up debugging through commit-push-wait cycles.
set -euo pipefail

cd "$(dirname "$0")/.."

# The pyspark wheel ships its own Spark distribution. A stale SPARK_HOME makes
# the JVM load a different one and fail in ways that never mention SPARK_HOME.
unset SPARK_HOME

# Fails loudly if uv.lock has drifted from pyproject.toml. This is the
# reproducibility gate: CI must install exactly what you installed.
uv sync --frozen

# Cheapest checks first, so a typo fails in seconds rather than minutes.
uv run ruff format --check .
uv run ruff check .
uv run mypy
uv run pytest -q

# PR 3 adds:  docker build -f docker/Dockerfile -t pipeline:local .
# PR 13 adds: [[ "${WITH_INTEGRATION:-0}" == "1" ]] && uv run pytest tests/integration
```

```bash
chmod +x ci/run.sh
```

Four decisions in there:

- **`set -euo pipefail`** — exit on error, on unset variables, and on a failure anywhere in a pipe. Without it a failing check in the middle is ignored and the script exits 0.
- **`cd "$(dirname "$0")/.."`** — the script works from any directory.
- **`uv sync --frozen`** rather than `uv sync`. Frozen *fails* if `uv.lock` has drifted from `pyproject.toml` instead of quietly re-resolving. That is the difference between CI installing what you installed and CI installing something new.
- **Cheapest first.** `ruff` and `mypy` take seconds and catch the errors a compiler would catch elsewhere. No point spending a minute on tests to discover an undefined name.

Note what the script does *not* reference: a Dockerfile, an integration suite.
Neither exists yet, and a CI script that calls a missing file is a broken `main`. The comments mark where they land.


## Step 2 — the `Makefile`

`ci/run.sh` defines what green means. The `Makefile` fixes the environment it runs in, and gives you shorter commands day to day.

**Recipes must be indented with real tabs**, not spaces. This is the one place in modern tooling where that still bites.

```makefile
SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

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
```

Then the targets — see the repo's `Makefile` for the full file. The interesting one is `env`, which validates rather than assumes:

```makefile
env: ## Show and validate the resolved environment
	@echo "JAVA_HOME  = $${JAVA_HOME:-(unset)}"
	@echo "SPARK_HOME = $${SPARK_HOME:-(unset)}   <- must be unset"
	@test -n "$${JAVA_HOME:-}" \
	  || { echo "ERROR: no JAVA_HOME. Install a JDK 17 or 21."; exit 1; }
	@test -x "$${JAVA_HOME}/bin/java" \
	  || { echo "ERROR: JAVA_HOME points at a path that does not exist: $${JAVA_HOME}"; exit 1; }
	@"$${JAVA_HOME}/bin/java" -version 2>&1 | head -1
```

`test` and `run` both depend on `env`, so a broken environment fails with a sentence you can act on instead of a Py4J stack trace.

Three mechanics worth knowing:

- **`unexport SPARK_HOME`** removes it from every recipe's environment, even when it is exported in your shell. This is the fix, applied once, for the whole project.
- **`export JAVA_HOME := $(shell /usr/libexec/java_home ...)`** resolves through a registry instead of a fixed path. Guarded by `uname -s` so it only runs on macOS, and skipped entirely if no suitable JDK is found, which lets the `env` target produce the good error message rather than make producing a cryptic one.
- **`$(CURDIR)`** is make's absolute path to the project, so `PYSPARK_PYTHON` is correct no matter where you invoke it from.


## Step 3 — GitHub Actions

`.github/workflows/ci.yml`:

```yaml
name: ci

on:
  pull_request:
  push:
    branches: [main]

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      # Yes, a Python project needs a JDK: pyspark starts a JVM. Leaving this
      # out is the mistake unique to a Python Spark project — it works locally
      # where a JDK happens to exist, then fails here with a gateway error that
      # never mentions Java.
      - uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: '21'

      - uses: astral-sh/setup-uv@v5
        with:
          enable-cache: true
          cache-dependency-glob: uv.lock

      # One command. Everything it runs is reproducible locally with `make ci`.
      - run: ./ci/run.sh
```

- **`setup-java` is not optional.** This is the failure unique to a Python Spark project: everything passes locally, where a JDK happens to exist, then CI dies with a gateway error that says nothing about Java.
- **`enable-cache` with `cache-dependency-glob: uv.lock`.** The PySpark wheel is a few hundred megabytes. Without caching every run re-downloads it.
- **The job runs one line.** No logic lives in the YAML, which is what keeps any CI failure reproducible with `make ci`.

The workflow does not call `make`. CI invokes `./ci/run.sh` directly, which is why the script unsets `SPARK_HOME` itself rather than relying on the Makefile.


## Step 4 — verify it, including the failure modes

A guard you have never seen fire is a guard you do not know works.

```bash
make help                       # the target list
make env                        # resolved environment, validated
make ci                         # everything CI runs
```

Now break the environment on purpose and confirm the Makefile survives it:

```bash
JAVA_HOME=/nonexistent/jdk SPARK_HOME=/some/old/spark make run
```

It should still print `spark 4.0.4` — the Makefile overrode both. And confirm the guard fails when it genuinely cannot find a JDK:

```bash
env -u JAVA_HOME make env DETECTED_JAVA_HOME=
# ERROR: no JAVA_HOME. Install a JDK 17 or 21.
```

```bash
make env DETECTED_JAVA_HOME= JAVA_HOME=/opt/homebrew/opt/openjdk@17
# ERROR: JAVA_HOME points at a path that does not exist: ...
```


## Definition of done

- [ ] `make ci` passes, and runs the same checks as `.github/workflows/ci.yml`
- [ ] `make run` works with a deliberately broken `JAVA_HOME` and `SPARK_HOME`
- [ ] `make env` fails with a readable message when no JDK can be found
- [ ] `ci/run.sh` is executable and references no file that does not exist yet
- [ ] `README.md` documents `make ci` rather than the four raw commands

Once the repo is on GitHub, turn on branch protection for `main` and require the `build` check. That is what turns "main is always runnable" into something the platform enforces rather than something you remember.

Next: **PR 3** — a multi-stage Dockerfile that resolves the environment with `uv` in one stage and copies it onto a Spark base image in the next, plus the `docker build` line in `ci/run.sh`.
