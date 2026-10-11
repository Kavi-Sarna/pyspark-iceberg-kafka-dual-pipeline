# pyspark-iceberg-kafka-dual-pipeline

[![Open in Github Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/Kavi-Sarna/pyspark-iceberg-kafka-dual-pipeline)

Dual-mode Pyspark pipeline - one pure join core running in both batch over Apache Iceberg and streaming over Kafka.

One join, written ince, running two ways: as a **batch** job over Apache Iceberg and as a **streaming** job over Kafka - off the same pure core.

The worked example is shipments and deliveries. A shipment is dispatched; a delivery event may follow. The question the pipeline answers is which shipments never arrived.

> **Status: PR 18 - complete.** One image runs either mode from a flag, configuration is layered and read once, streaming metrics reach Grafana, the two modes deploy to a `kind` cluster through the Spark Operator, and six operational drills are recorded with what happened and why.
> 
> See [Status](#status) and [Drills](docs/drills.md).

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
| Skeleton on a real cluster     | 1-4   | **done**     |
| Pure core, no I/O              | 5-6   | **done**     |
| Batch path on Iceberg          | 7-9   | **done**     |
| Streaming path on Kafka        | 10-13 | **done**     |
| One artifact, two modes        | 14    | **done**     |
| Observability and Kubernetes   | 15-17 | **done**     |
| Operational drills             | 18    | **done**     |


## Stack

|              |                                                                                            |
| ------------ | ------------------------------------------------------------------------------------------ |
| Python       | 3.12, managed with `uv`                                                                    |
| Spark        | 4.0.4 (the `pyspark` wheel ships the distribution)                                         |
| Table format | Apache Iceberg 1.11 - *not* Spark 4.1, which has no Iceberg runtime                        |
| Object Store | Silo (`pgsty/minio`), the maintained fork - upstream MinIo was deleted from Docker Hub     |
| Stream       | Kafka + Confluent Schema Registry, Avro                                                    |
| Image        | `apache/spark:4.0.4-python3`, multi-stage, non-root uid 185                                |
| Local stack  | Docker Compose -> `kind` + the Spark Operator                                              |
| Checks       | `ruff`, `mypy --strict`, `pytest`                                                          |


## Quick start

You need a JDK 17 or 21 - Spark runs on the JVM even though this is a Python project - plus [uv](https://docs.astral.sh/uv/). Nothing else: the `pyspark` wheel ships its own Spark distribution.

```bash
make env           # resolves and validates JAVA_HOME, fails loudly if it can't
make run
```

Two things worth running before anything needs Docker:

```bash
make local              # create, seed and run the batch job in a directory warehouse
make watermark-demo     # watch an unmatched row wait in state, then be released
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


## Local cluster

```bash
make up         # master, worker and History Server, all healthy
make submit     # spark-submit the app to it
make down       # stop and drop volumes

make produce    # send a scenario to Kafka, SCENARIO=unmatched|watermark|...
make stream     # run the streaming join; AWAIT_SECONDS=90 to stop after a while
make batch      # bootstrap, then run the job on the cluster into Iceberg
make smoke      # all of the above, with assertions, then teardown
```

|                           |                                           |
| ------------------------- | ----------------------------------------- |
| Master UI                 | http://localhost:8080                     |
| MinIO console             | http://localhost:9001                     |
| Iceberg REST catalog      | http://localhost:8181                     |
| Schema Registry           | http://localhost:8085                     |
| Kafka, from the host      | http://localhost:29092                    |
| Worker UI                 | http://localhost:8081                     |
| History Server            | http://localhost:18080                    |
| Grafana                   | http://localhost:3000/d/pipeline-stream   |
| Prometheus                | http://localhost:9090                     |
| statsd-exporter           | http://localhost:9102/metrics             |

Every service runs the image this repo builds, not a stock Spark image, so the cluster executes your code and your Python. A dependency missing from the image fails at the build rather than as a task failure on an executor.

`make smoke` is the check behind *`main` is always runnable*: it asserts the job reported the cluster master, and that the master itself recorded a completed application. Exit zero alone would pass even if the job quitely ran locally.


## Drills

Six operational questions, answered by running them rather than by reasoning about them: restart a query, delete its checkpoint, change its shape, evolve an Avro schema, compact and expire an Iceberg table, and swap the state store for RocksDB.

```bash
make drills
```

Results and reasoning: [`docs/drills.md`](docs/drills.md). Three of them have answers most people guess wrong — deleting a checkpoint duplicates output rather than starting clean, a *required* field added to a writer is invisible to an old reader while an *optional* field added to a reader breaks it, and compaction frees no space at all until snapshots expire.


## Kubernetes

```bash
make k8s-validate   # check the manifests; no cluster needed
make kind-up        # cluster, Spark Operator, image side-loaded
make k8s-up         # MinIO, catalog, Kafka, registry, then topics and tables
make k8s-stream     # the streaming join as a SparkApplication
make k8s-batch      # the batch join, every ten minutes
make k8s-logs       # follow the driver
make kind-down      # destroy it all
```

Every target that touches a cluster refuses to run unless the current kubectl context is `kind-pipeline`. These manifests carry demo credentials and two of the targets delete things.

The Services are named `minio`, `iceberg-rest`, `kafka` and `schema-registry` - the Compose service names, which are the hostnames already in the packaged `app.toml`. Nothing about the job's configuration changes between the two.


## Running it without installing anything 

Step by step, including how to share a live URL: [`docs/pr-04-1-walkthrough.md`](docs/pr-04-1-walkthrough.md).

Click the badge at the top. Github builds a codespace, installs a JDK and `uv`, builds the image and starts the cluster, and forwards the Spark UIs - no Docker, no JDK and no clone on your machine.

The first create takes several minutes; a resume takes seconds. A codespace stops itself after 30 minutes idle and burns no compute while stopped. On the free tier a 2-core machine is about 60 wall-clock hours a month.

Ports are private to you by default. To show someone, set port 8080 to **Public** in the Ports panel for as long as the demo lasts. The Spark UIs have no authentication, so that URL is open to anyone who has it.

`.devcontainer/on-create.sh` does the one-off build and 
`.devcontainer/post-start.sh` brings the cluster up on every start, including after an idle stop - a create-only hook would leave the second visit dead.


## Design rules

Three contraints shape every PR, and they are the reason the ladder is 18 steps rather than three.

- **`main` is always runnable.** Every commit builds, passes CI and runs. New code lands inert and is wired in last, so no intermediate state is broken.
- **Declare every schema, infer nothing.** Inference throws on empty input, costs a pass over the data, and makes the schema a function of whatever arrived rather than a contract. The app must run correctly against an empty stack, which is what the smoke test checks.
- **`ruff` and `mypy` are not optional.** Python has no compiler, so they do the job a compiler does for free elsewhere: catching breakage before anything runs.


## Layout

```
src/pipeline/main.py        entry point: build a session, report, stop
src/pipeline/session.py     entry point's session builder plus catalog config
src/pipeline/bootstrap.py   creates the namespace and tables from the schemas
src/pipeline/core/          pure domain logic: schema.py, join.py
src/pipeline/config.py      all configuration: packages TOML, file, environment
src/pipeline/conf/          the shipped defaults
src/pipeline/streams.py     Kafka topics and Schema Registry subjects
src/pipeline/producer.py    scenarios with fixed event times and explicit pauses
src/pipeline/avro/          the wire contract, as .avsc
src/pipeline/io/            adapters: iceberg.py, kafka.py
src/pipeline/runner/        assembly: batch.py, stream.py
tests/conftest.py           one session-scoped SparkSession for the suite
tests/unit/                 no Docker required
tests/integration/          real Iceberg tables, the watermark experiment, a real broker
docs/drills.md              six drills: what was done, what happened, why
ci/run.sh                   the pipeline contract - what "green" means
ci/k8s.sh                   manifest against the published Kubernetes and operator schemas
ci/scan.sh                  image vulnerability scan, run nightly not per-PR
ci/smoke.sh                 end-to-end: submit to the cluster and assert 
src/pipeline/metrics.py     a StreamingQueryListener and a StatsD client
k8s/                        kind cluster, the Spark Operator CRs, and what they talk to
docker/compose.yaml         local cluster: master, worker, History Server
docker/metrics/             statsd mapping, Prometheus scrape, Grafana dashboard
.devcontainer/              one-click Codespace demo, cluster up on start
.github/workflows/          ci.yml calls run.sh; scan.yml schedules scan.sh
Makefile                    thin wrapper; pins the environment
docker/Dockerfile           multi-stage image on the official Spark runtime
.dockerignore               keeps .venv and caches out of the build context
docs/                       per-PR walkthroughs
```


## Docs

- [`docs/pr-01-walkthrough.md`](docs/pr-01-walkthrough.md) - build PR 1 yourself, step by step 
- [`docs/pr-02-walkthrough.md`](docs/pr-02-walkthrough.md) - CI, the Makefile, and pinning the environment 
- [`docs/pr-03-walkthrough.md`](docs/pr-03-walkthrough.md) - the container image, and the Python version clash at its center 
- [`docs/pr-04-walkthrough.md`](docs/pr-04-walkthrough.md) - a real cluster, and the guard that never fired 
- [`docs/pr-04-1-walkthrough.md`](docs/pr-04-1-walkthrough.md) - Codespaces setup, and sharing a live demo URL 
- [`docs/pr-05-walkthrough.md`](docs/pr-05-walkthrough.md) - declared schemas, and why a left join relaxes nullability
- [`docs/pr-06-walkthrough.md`](docs/pr-06-walkthrough.md) - the join, the duplicate key column, and the post-filter that becomes an inner join
- [`docs/pr-07-walkthrough.md`](docs/pr-07-walkthrough.md) - the Iceberg catalog, getting jars onto a Python job's classpath, and MinIO's disappearance
- [`docs/pr-08-walkthrough.md`](docs/pr-08-walkthrough.md) - the first end-to-end run, and why `--packages` moves to the submit command
- [`docs/pr-09-walkthrough.md`](docs/pr-09-walkthrough.md) - hidden partitioning, what it actually buys measured, and the idempotent re-run
- [`docs/pr-10-walkthrough.md`](docs/pr-10-walkthrough.md) - Kafka in KRaft mode, the two listener problem, and keeping Avro and Spark schema in step
- [`docs/pr-11-walkthrough.md`](docs/pr-11-walkthrough.md) - a producer with fixed event times, and the Confluent wire format written by hand
- [`docs/pr-12-walkthrough.md`](docs/pr-12-walkthrough.md) - the streaming join: why watermarks are not enough. and the five bytes that corrupt silently
- [`docs/pr-13-walkthrough.md`](docs/pr-13-walkthrough.md) - the watermark experiment as a test, and looking inside the state store
- [`docs/pr-14-walkthrough.md`](docs/pr-14-walkthrough.md) - layered configuration, and why the two absent keys are the switches
- [`docs/pr-15-walkthrough.md`](docs/pr-15-walkthrough.md) - event logs: rolling is already on, retention is what actually bounds
- [`docs/pr-16-walkthrough.md`](docs/pr-16-walkthrough.md) - streaming metrics: why there is no consumer group to check, and where lag actually lives
- [`docs/pr-17-walkthrough.md`](docs/pr-17-walkthrough.md) - the Spark Operator: what the executors inhert, and a manifest that validates but cannot run
- [`docs/pr-18-walkthrough.md`](docs/pr-18-walkthrough.md) - six operational drills, and the schema change that corrupts without an error
- [`docs/drills.md`](docs/drills.md) - the drill results on their own
- [Left Join Launchpad](https://gist.github.com/Kavi-Sarna/a06d702eb88946c7c4ab195a15529449) - the full 18-PR roadmap, concept map and traps 
- [Ground School](https://gist.github.com/Kavi-Sarna/24d6508a6296913cd790e6e3fa4a76b9) - prerequisities, worked exercises and an answer key 
