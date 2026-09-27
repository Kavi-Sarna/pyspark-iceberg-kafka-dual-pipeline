# pyspark-iceberg-kafka-dual-pipeline

Dual-mode Pyspark pipeline - one pure join core running in both batch over Apache Iceberg and streaming over Kafka.

One join, written ince, running two ways: as a **batch** job over Apache Iceberg and as a **streaming** job over Kafka - off the same pure core.

The worked example is shipments and deliveries. A shipment is dispatched; a delivery event may follow. The question the pipeline answers is which shipments never arrived.

> **Status: PR 3 - container image.** A SparkSession builds, reports itself and stops; `make ci` runs the same checks Github Actions does; and the app now ships as an image built on the official Spark runtime. None of the pipeline below exists yet. 
> 
> See [Status](#status).

## The idea

Everything that touches the outside world lives in thin adapters at the edges.
The join itself is a pure function on DataFrames that knows nothing about Kafka, Iceberg, checkpoints, or which mode it is running in.

```
STREAM kafka: shipments -> io/kafka.py (avro decode + watermark) ----
                                                                    |
BATCH iceberg: shipments -> io/iceberg.py (declared schema) ---------
                                                                    |
       |----------------------------------------------------------------|
       | core/join.py - left_join(shipments, deliveries, spec)          |
       | Pure. No session, no I/O, no config. Unit-testable in memory.  |
       |----------------------------------------------------------------|
                                                                    |
SINKS       iceberg sink / kafka sink ----------------------------------> table / topic
```

Two runners assemble those pieces and are about fifteen lines each. `main.py` reads a mode flag and picks one.

The interesting part is where that abstraction leaks: a stream-stream left outer join fails at analysis time unless both sides carry a watermark *and* the join condition is time-bounded, while batch has no such requirement. The difference is passed in as data rather than branched on inside the core.


## Status

| Phase                          | PRs   | State        |
| ------------------------------ | ----- | ------------ |
| Skeleton on a real cluster     | 1-4   | PRs 1-3 done |
| Pure core, no I/O              | 5-6   | - |
| Batch path on Iceberg          | 7-9   | - |
| Streaming path on Kafka        | 10-13 | - |
| One artifact, two modes        | 14    | - |
| Observability and Kubernetes   | 15-17 | - |
| Operational drills             | 18    | - |


## Stack

|              |                                                                   |
| ------------ | ----------------------------------------------------------------- |
| Python       | 3.12, managed with `uv`                                           |
| Spark        | 4.0.4 (the `pyspark` wheel ships the distribution)                |
| Table format | Apache Iceberg 1.11 - not Spark 4.1, which has no Iceberg runtime |
| Stream       | Kafka + Confluent Schema Registry, Avro                           |
| Image        | `apache/spark:4.0.4-python3`, multi-stage, non-root uid 185       |
| Local stack  | Docker Compose -> `kind` + the Spark Operator                     |
| Checks       | `ruff`, `mypy --strict`, `pytest`                                 |


## Quick start

You need a JDK 17 or 21 - Spark runs on the JVM even though this is a Python project - plus [uv](https://docs.astral.sh/uv/). Nothing else: the `pyspark` wheel ships its own Spark distribution.

```bash
make env           # resolves and validates JAVA_HOME, fails loudly if it can't
make run
```

```
INFO pipeline: spark    4.0.4
INFO pipeline: python   3.12.12
INFO pipeline: master   local[*]
```


## Checks

```bash
make ci         # exactly what CI runs
```

`ci/run.sh` is the single source of truth: lockfile check, format, lint, types, tests, image build. Github Actions calls that one script and nothing else, so any CI failure reproduces locally with one command.

The image build is the one conditional step. CI sets `REQUIRE_DOCKER=1`, so a missing daemon fails the build there. Locally it is skipped with a warning - a machine without Docker running should still be able to check everything else.

Individually: `make fmt`, `make lint`, `make types`, `make test`. `make help` lists everything, `make env` shows and validates the resolved environment.

The `Makefile` pins `JAVA_HOME`, unexports `SPARK_HOME` and fixes `PYSPARK_PYTHON`, so the job does not inherit whatever your shell happens to hold. That is why `make run` works even when your shell is misconfigured.


## Container

```bash
make image          # docker build -f docker/Dockerfile -t pipeline:local .
make image-run      # same output as `make run`, inside the image
make scan           # fixable HIGH/CRITICAL findings, via trivy
```

The image is built in two stages, both on `apache/spark:4.0.4-python3`. The first uses `uv` to download a self-contained Python 3.12 and resolve the locked dependencies against it; the second copies the interpreter and the venv onto a clean copy of the same base. That is what lets the container run the same interpreter your laptop does, rather than relaxing the pin to match the 3.10 the base image ships.

Both stages share a base so there is one image to track for vulnerabilities and one to bump, and so the venv is built against the glibc it will run on. Only the runtime stage ships: the final image is the last `FROM` plus the two paths copied from the builder, so nothing deom the builder's package manager or caches is in the artifact.

`ci/scan.sh` scans that image for fixable HIGH or CRITICAL findings. It runs **nightly against `main`**, not on pull requests - a vulnerability fedd changes daily, so gating a PR on it fails unchanged diffs for reasons the diff did not cause, and the base image is Apache's rather than ours to fix. Trigger it by hand from the Actions tab, or run `make scan` locally.


## Design rules

Three contraints shape every PR, and they are the reason the ladder is 18 steps rather than three.

- **`main` is always runnable.** Every commit builds, passes CI and runs. New code lands inert and is wired in last, so no intermediate state is broken.
- **Declare every schema, infer nothing.** Inference throws on empty input and silently guesses wrong types on a partial sample. The app must run constantly against an empty stack, which is what the smoke test checks.
- **`ruff` and `mypy` are not optional.** Python has no compiler, so they do the job a compiler does for free elsewhere: catching breakage before anything runs.


## Layout

```
src/pipeline/main.py        entry point: build a session, report, stop
tests/unit/                 no Docker required
ci/run.sh                   the pipeline contract - what "green" means
ci/scan.sh                  image vulnerability scan, run nightly not per-PR
.github/workflows/          ci.yml calls run.sh; scan.yml schedules scan.sh
Makefile                    thin wrapper; pins the environment
docker/Dockerfile           multi-stage image on the official Spark runtime
.dockerignore               keeps .venv and caches out of the build context
docs/                       per-PR walkthroughs
```


## Docs

- [`docs/pr-01-walkthrough.md`](docs/pr-01-walkthrough.md) - build PR 1 yourself, step by step
- [`docs/pr-02-walkthrough.md`](docs/pr-02-walkthrough.md) - CI, the Makefile, and pinning the environment.
- [`docs/pr-03-walkthrough.md`](docs/pr-03-walkthrough.md) - the container image, and the Python version clash at its center
- [Left Join Launchpad](https://gist.github.com/Kavi-Sarna/a06d702eb88946c7c4ab195a15529449) - the full 18-PR roadmap, concept map and traps
- [Ground School](https://gist.github.com/Kavi-Sarna/24d6508a6296913cd790e6e3fa4a76b9) - prerequisities, worked exercises and an answer key
