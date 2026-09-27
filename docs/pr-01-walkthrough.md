# PR 1 — walkthrough

Build the skeleton of **dual-mode-pyspark-iceberg-pipeline** yourself. Every command and file is here, and every code block is byte-identical to the working reference implementation.

**Goal:** a `uv` project whose `main.py` opens a SparkSession, reports what it is running on, and stops cleanly — with lint, types and tests all green.

**Not in this PR:** `ci/run.sh`, `Makefile`, Dockerfile, Compose. Those are PRs 2–4.


## Step 0 — fix the environment first

Two variables decide whether any of this works, and on this machine both were wrong. Check them before writing a line of code.

```bash
echo "JAVA_HOME=$JAVA_HOME"
[ -x "$JAVA_HOME/bin/java" ] && echo "  exists" || echo "  DANGLING"
echo "SPARK_HOME=${SPARK_HOME:-(unset)}"
```

**Problem 1 — `JAVA_HOME` points at a JDK that is gone.** `~/.zshenv` line 6 sets it to `/opt/homebrew/opt/openjdk@17`, but Homebrew now only carries `openjdk` and `openjdk@26`; the `@17` formula was removed. Every JVM tool on this machine that
trusts `JAVA_HOME` is broken, not just Spark.

Fix it so it resolves through Apple's JDK registry instead of a Homebrew path that can disappear:

```bash
# in ~/.zshenv, replace line 6 with:
export JAVA_HOME="$(/usr/libexec/java_home -v 21)"
```

**Problem 2 — `SPARK_HOME` points at an old Spark.** `~/.zshrc` line 20 sets it to `/usr/local/src/spark-3.4.0-bin-hadoop3`. The `pyspark` wheel ships its own Spark 4.0.4; if `SPARK_HOME` is set, the JVM loads the 3.4.0 jars instead and dies.

Remove those two lines from `.zshrc`, or unset it per shell:

```bash
unset SPARK_HOME
```

Verify before continuing:

```bash
java -version                 # 17.x or 21.x
ls "$JAVA_HOME/bin/java"
echo "${SPARK_HOME:-unset}"   # must print: unset
```

> PR 2's `Makefile` pins both, so the project stops caring what your shell holds.
> Until then you are relying on the shell being right.


## Step 1 — scaffold with uv

`uv init --package` creates the `src/` layout for you, so you are not hand-rolling the structure:

```bash
mkdir -p ~/Code/Self/dual-mode-pyspark-iceberg-pipeline
cd ~/Code/Self/dual-mode-pyspark-iceberg-pipeline
uv init --package --name pipeline
```

That gives you:

```
.python-version             pins the interpreter for uv
pyproject.toml              project definition (we rewrite most of it in Step 2)
README.md                   placeholder
.gitignore                  a starter, which we extend in Step 5
src/pipeline/__init__.py    an empty package, ready for code
```

Two flags worth knowing. Plain `uv init` would give you a **flat** layout — a lone `main.py` at the root, no package — which is the shape we ruled out earlier: top-level module names collide with the standard library and with each other once `core/`, `io/` and `runner/` arrive. `--package` is what produces `src/pipeline/`. And `--name pipeline` sets the *import* name independently of the directory name, which matters here because the repo is called something much longer.

Now add the directories the scaffold does not create:

```bash
mkdir -p tests/unit docs
```


## Step 2 — `pyproject.toml`

The generated file is a stub. Replace it with this — one file defining dependencies, the Python version, and the config for every tool:

```toml
[project]
name = "pipeline"
version = "0.1.0"
description = "Shipments LEFT JOIN deliveries, in batch and streaming mode, off one shared core."
requires-python = ">=3.12,<3.13"
dependencies = [
    "pyspark==4.0.4",
]

[dependency-groups]
dev = [
    "mypy>=1.11",
    "pytest>=8",
    "ruff>=0.6",
]

[project.scripts]
pipeline = "pipeline.main:main"

[build-system]
requires = ["uv_build>=0.10.6,<0.11.0"]
build-backend = "uv_build"

[tool.ruff]
line-length = 100
src = ["src", "tests"]

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "SIM"]

[tool.mypy]
python_version = "3.12"
strict = true
files = ["src", "tests"]

[tool.pytest.ini_options]
testpaths = ["tests"]
```

What changed from the generated stub, and why:

- **`dependencies = ["pyspark==4.0.4"]`**, pinned exactly. The wheel bundles the whole Spark distribution, so this pin fixes your engine version too. Not `>=`: a silent minor bump would change query behavior.
- **`requires-python = ">=3.12,<3.13"`** rather than the generated `>=3.12`. PySpark 4.0.4 supports 3.9–3.13 and `testcontainers` (PR 13) needs 3.10+; pinning one minor version keeps the lockfile reproducible.
- **`[dependency-groups] dev`** for tooling, so `ruff`, `mypy` and `pytest` never ship in the runtime image.
- **`[project.scripts]`** corrected from the generated `pipeline = "pipeline:main"` to `"pipeline.main:main"`, since our entry point lives in `main.py`. This gives you `uv run pipeline` as a shorthand for `uv run python -m pipeline.main`.
- **`mypy strict`.** Python has no compiler, so `ruff` and `mypy` are the only things standing between a typo and a 3am failure. Strict from PR 1 is far cheaper than retrofitting it at PR 10.

`uv init --package` selects the `uv_build` backend, which is what we keep here. `hatchling` is the more common choice across the wider ecosystem and works identically for this project — swap it if you would rather learn that one.


## Step 3 — the package

`src/pipeline/__init__.py` — the scaffold created this empty; give it a docstring:

```python
"""Shipments LEFT JOIN deliveries, in batch and streaming mode."""

__version__ = "0.1.0"
```

`src/pipeline/main.py`:

```python
"""Entry point.

Builds a SparkSession, reports what it is running on, and stops cleanly.
"""

from __future__ import annotations

import logging
import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession

log = logging.getLogger("pipeline")


def build_session(app_name: str = "pipeline") -> SparkSession:
    """Build a session, defaulting to local[*] only when no master is configured.

    spark-submit sets spark.master in the conf; overriding it here would ignore
    whatever cluster the job was submitted to.
    """
    builder = SparkSession.builder.appName(app_name)
    if not SparkConf().contains("spark.master"):
        builder = builder.master("local[*]")
    return builder.getOrCreate()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        log.info("spark   %s", spark.version)
        log.info("python  %s", sys.version.split()[0])
        log.info("master  %s", spark.sparkContext.master)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

Three things that look like detail and are not:

- **`build_session` is separate from `main`.** Tests need a session without running the program. Every later PR builds on this split.
- **The master check.** Hardcoding `.master("local[*]")` would override whatever `spark-submit` was told in PR 4, silently running your cluster job locally. Setting it only when nothing else has is the behavior you want in both places.
- **`spark.stop()` in a `finally`.** A leaked SparkContext keeps a JVM and its port alive; the next run then collides with it.


## Step 4 — the test

`tests/__init__.py` and `tests/unit/__init__.py` — both empty.

`tests/unit/test_main.py`:

```python
"""PR 1 only proves the toolchain: a session builds, reports itself, and stops."""

from __future__ import annotations

from pipeline.main import build_session


def test_session_builds_and_reports_spark_4() -> None:
    spark = build_session("pipeline-test")
    try:
        assert spark.version.startswith("4.0")
    finally:
        spark.stop()


def test_main_exits_zero() -> None:
    from pipeline.main import main

    assert main() == 0
```

Thin, deliberately. There is no logic yet, so the only thing worth asserting is that the toolchain works — which is exactly the failure these two would have caught in Step 0.


## Step 5 — `.gitignore`

`uv init` wrote a starter covering `.venv/` and `__pycache__/`. Extend it so the tool caches and Spark's local artifacts are covered too:

```gitignore
.venv/
__pycache__/
*.py[cod]
.pytest_cache/
.mypy_cache/
.ruff_cache/

# Spark local artifacts
spark-warehouse/
metastore_db/
derby.log
```

The Spark entries matter: a local run drops `spark-warehouse/`, `metastore_db/` and `derby.log` into the working directory, and they should never be committed.


## Step 6 — resolve and install

```bash
uv sync
```

Creates `.venv`, installs everything, and writes `uv.lock`. The first run downloads the PySpark wheel — a few hundred megabytes, once.

**Commit `uv.lock`.** It records the exact resolved version of every transitive dependency; without it your machine and CI can quietly install different code.


## Step 7 — run the checks

Four commands, in ascending order of cost. This ordering is what `ci/run.sh` formalizes in PR 2.

```bash
uv run ruff format --check .   # 5 files already formatted
uv run ruff check .            # All checks passed!
uv run mypy                    # Success: no issues found in 5 source files
uv run pytest -q               # 2 passed
```


## Step 8 — run it

```bash
uv run python -m pipeline.main
```

```
INFO pipeline: spark   4.0.4
INFO pipeline: python  3.12.12
INFO pipeline: master  local[*]
```

Spark prints warnings above that — `WARNING: Using incubator modules`, `NativeCodeLoader: Unable to load native-hadoop library`. Both are normal on every Spark start and are not errors.


## Step 9 — git

```bash
git init
git add -A
git status --short
```

Check that `.venv/` is **not** listed. Then stop: review the staged diff and record it only when you are happy with it.


## Definition of done

- [ ] `ruff format --check`, `ruff check`, `mypy`, `pytest` all pass
- [ ] `uv run python -m pipeline.main` prints Spark 4.0.4 and exits 0
- [ ] `uv.lock` is tracked
- [ ] `.venv/` is not tracked
- [ ] `README.md` replaces the generated placeholder and says how to run it


---

Next: **PR 2** — `ci/run.sh`, a `Makefile` that pins `JAVA_HOME` and unsets `SPARK_HOME`, and `.github/workflows/ci.yml`. That PR is what makes Step 0 unnecessary for whoever clones this next.
