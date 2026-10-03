# PR 7 — walkthrough

**Goal:** an Iceberg catalog the job can write to — object storage and a REST catalog in Compose, the Iceberg jars on a Python job's classpath, and a bootstrap that creates the tables from the declared schemas.

**Why now:** the core is finished and tested. This is the first half of giving it somewhere to read from and write to, split from the reading and writing itself so that when something breaks you know which half.

**Not in this PR:** `io/iceberg.py`, the batch runner, partitioning.

> **Verification status.** More is verified here than the Docker situation suggests. The jars, the Iceberg extensions, table creation, a write, a snapshot and the full on-disk metadata chain were all exercised for real against a directory-backed catalog — `make warehouse` does exactly that and needs no Docker. What has **not** run is MinIO, the REST catalog and `make bootstrap`, because those need a daemon.


## Step 0 — MinIO is gone

The roadmap said MinIO. That is no longer possible, and the story is worth knowing because it will happen to something else you depend on.

MinIO stopped publishing free community images in October 2025, archived the open-source repository in February 2026, and **deleted `minio/minio` and `minio/mc` from Docker Hub on 2026-09-11**. Verified:

```
docker.io/minio/minio        -> 404 (repository does not exist)
quay.io/minio/minio          -> 401 for every tag, including RELEASE.2025-04-22
ghcr.io/minio/minio          -> 403
public.ecr.aws/minio/minio   -> 404
```

The 401 on Quay deserves a note on method. Quay returns 401 for a missing *and* for a private repository, so a bare 401 proves nothing. Running the same token flow against `quay.io/prometheus/prometheus` returns 200, which establishes that the method works and the MinIO result is real. **Always test your probe against a known-good control before concluding from a negative.** Much of the advice online still says "just repoint to Quay"; that advice is already stale.

### What people moved to

|                           |                                                                       |
| ------------------------- | --------------------------------------------------------------------- |
| `alpine/minio`            | A rebuild by an individual. The `alpine` Docker Hub org is one person's account, unverified, and unrelated to Alpine Linux. Works, but it is a stranger's binary and frozen at October 2025.                  |
| `pgsty/minio`             | **Silo** — Pigsty's AGPL fork of MinIO. A real project with governance, a stated cadence of a release every one to two months, and a companion `pgsty/mc`.                                                        |
| `rustfs/rustfs`           | Apache-2.0, Rust, explicitly a MinIO drop-in. Laravel Sail picked it. Only reached 1.0 recently. |
| `chrislusf/seaweedfs`     | A different architecture with an S3 gateway. Most pulls of the lot and very actively maintained. |
| `dxflrs/garage`           | Small and good, but a multi-step bootstrap and no recent semver tags. |

This project uses **Silo**, for three reasons: it is MinIO's codebase, so every config key and every tutorial still applies; it ships `mc`, so bucket creation is the normal one-liner rather than a workaround; and its provenance is a known open-source project rather than an anonymous rebuild.

It is still a community fork of an archived upstream, so it is pinned by digest:

```yaml
    image: pgsty/minio:RELEASE.2026-08-04T00-00-00Z@sha256:b6bfe7239bfc83fb90d31612d9704d86039dd714f7904b3f1ad68f211e602372
```

The tag says what it is, the digest makes it immutable. For a local stack with synthetic data and nothing exposed, a forked object store is a reasonable risk.
It would not be for anything holding real data, and the comment in `compose.yaml` says so.


## Step 1 — how jars reach a Python job's classpath

This is the practical thing to understand in this PR. PySpark is a thin client over a JVM, and Iceberg is a JVM library. Your Python code cannot import it; the JVM has to have it on the classpath before it starts.

```python
PACKAGES = (
    f"org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:{ICEBERG_VERSION},"
    f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}"
)
```

Three things encoded in those coordinates:

- **`spark-runtime-4.0`** must match your Spark minor version. This is why the project pins Spark 4.0 and not 4.1: Iceberg publishes no 4.1 runtime.
- **`_2.13`** is the Scala version Spark was built with. Get it wrong and you get `NoSuchMethodError` at runtime, not a resolution failure.
- **`iceberg-aws-bundle`** carries the AWS SDK that `S3FileIO` needs. It is a separate artifact and a separate chance to get the version wrong, which is why a test asserts both coordinates pin the same version.

The surprising part is that setting this through the builder works at all:

```python
builder = builder.config("spark.jars.packages", PACKAGES)
```

`spark.jars.packages` is read when the JVM launches, and in PySpark the JVM has not launched yet at `SparkSession.builder` time — pyspark starts it on `getOrCreate()`, folding these settings into the launch. Set the same property *after* a session exists and it is silently ignored, which is a common and confusing way to lose an afternoon.

*Verified, without Docker:*

```
PROBE rows: 1
PROBE snapshots: 1
```

Jars resolved from Maven, `IcebergSparkSessionExtensions` loaded, a table created, a row written, a snapshot recorded.


## Step 2 — the catalog config, and the gate

Catalog settings are a flat pile of strings that Spark does not validate. A typo gives you a silently ignored option or a `ClassNotFoundException` at first use, so they are generated by a pure function and asserted by tests:

```python
def catalog_options(cfg: CatalogConfig) -> dict[str, str]:
    """The Spark configuration for one Iceberg catalog."""
    at = f"spark.sql.catalog.{cfg.name}"
    options = {
        "spark.jars.packages": PACKAGES,
        "spark.sql.extensions": EXTENSIONS,
        at: "org.apache.iceberg.spark.SparkCatalog",
        f"{at}.type": cfg.type,
        f"{at}.warehouse": cfg.warehouse,
    }
    if cfg.uri:
        options[f"{at}.uri"] = cfg.uri
    if cfg.s3_endpoint:
        # path-style because a bucket-as-subdomain needs DNS the local stack
        # does not have.
        options[f"{at}.io-impl"] = "org.apache.iceberg.aws.s3.S3FileIO"
        options[f"{at}.s3.endpoint"] = cfg.s3_endpoint
        options[f"{at}.s3.path-style-access"] = "true"
        options[f"{at}.client.region"] = cfg.region
    return options
```

`path-style-access` is the one that catches people. AWS S3 addresses a bucket as `bucket.s3.amazonaws.com`; a local MinIO has no wildcard DNS, so the client must use `endpoint/bucket` instead. Without it you get a connection error naming a hostname that was never going to resolve.

### The gate

```python
    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> CatalogConfig | None:
        """None when nothing is configured, so a bare `make run` stays offline."""
```

No `ICEBERG_WAREHOUSE`, no catalog, no `spark.jars.packages`, no Maven download. This is the "land it inert" rule again: adding Iceberg must not make `make run` reach the network on a laptop that only wanted to see a version number.

*Verified:*

```
INFO pipeline: master  local[*]
INFO pipeline: catalog none
```

Taking `env` as an argument rather than reading `os.environ` inside is what makes the whole thing unit-testable with no monkeypatching.


## Step 3 — the Compose services

```yaml
  minio-init:
    image: pgsty/mc:RELEASE.2026-09-16T00-00-00Z@sha256:cfc83108c3abb371f8fb84d99c1fdc88f8c237e022409b0081fb7c0a3be634dd
    depends_on:
      - minio
    entrypoint: ["/bin/sh", "-c"]
    command:
      - >-
        until mc alias set local http://minio:9000 pipeline-local pipeline-local-secret;
        do echo "waiting for minio"; sleep 1; done &&
        mc mb --ignore-existing local/warehouse &&
        echo "bucket ready"
```

A deliberate choice here: MinIO and the REST catalog have **no healthchecks**.
PR 4 argued healthchecks are how you avoid races, so this needs a reason.

A healthcheck runs *inside* the container, so it can only use tools that image happens to ship. The Spark image has `wget`, which is why PR 4's checks work. For these two images I cannot verify what is inside without a daemon, and a healthcheck referencing a missing binary fails forever and reads exactly like a broken service. So the ordering is expressed differently: `minio-init` retries
until MinIO answers, and `iceberg-rest` waits on `condition: service_completed_successfully`. The retry loop *is* the readiness check, and it needs nothing extra.

Note what the credentials are not: `minioadmin/minioadmin`, the well-known default. They are obviously local-only strings, they exist in one file, and nothing else in the project reuses them.


## Step 4 — the bootstrap, from the schemas

```python
def create_tables(spark: SparkSession, catalog: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{NAMESPACE}")
    for table, schema in TABLES.items():
        fqn = f"{catalog}.{NAMESPACE}.{table}"
        spark.sql(f"CREATE TABLE IF NOT EXISTS {fqn} ({schema.toDDL()}) USING iceberg")
        log.info("ready   %s", fqn)
```

`StructType.toDDL()` is the point. The alternative is a second copy of the column list in SQL, which is a schema drift waiting to happen: change `schemas.py`, forget the DDL, and the table no longer matches what the code writes. Here there is one definition.

```python
SHIPMENT.toDDL()
# shipment_id STRING NOT NULL,carrier STRING NOT NULL,dispatch_time TIMESTAMP NOT NULL
```

One catch worth knowing: `toDDL()` needs an active SparkSession, because it calls into the JVM. It fails with `SESSION_OR_CONTEXT_NOT_EXISTS` if you try it at import time.

### Two things the created table reveals

*Verified* by reading the metadata JSON after `make warehouse`:

```
shipments:  [('shipment_id', 'string', True), ('carrier', 'string', True),
             ('dispatch_time', 'timestamptz', True)]
```

**`required=True` survived.** The `NOT NULL` from PR 5's declaration is now a property of the table, not just of the DataFrame. The schema contract reaches storage.

**`TIMESTAMP` became `timestamptz`.** Spark's `TimestampType` means timestamp-with-local-timezone, and Iceberg maps it to `timestamptz` rather than `timestamp`. That is usually what you want, but it is a real difference and it surfaces later when something else reads the table — a query engine will apply a session timezone you did not think about. If you genuinely want a wall-clock timestamp with no zone, the Spark type is `TimestampNTZType`, and choosing it is a decision rather than a default.


## Step 5 — two ways to run it

```bash
make bootstrap   # against the Compose stack: MinIO + REST catalog
make warehouse   # against a local directory: no Docker, no services
```

`make warehouse` exists because a catalog does not require object storage. A `hadoop`-type catalog is just a directory, which makes it the fastest way to learn the Iceberg metadata chain and the only way to exercise any of this while Docker is down.

*Verified:*

```
INFO pipeline.bootstrap: ready   lake.ops.shipments
INFO pipeline.bootstrap: ready   lake.ops.deliveries
```

Then look at what Iceberg actually wrote:

```bash
find .warehouse -type f | sort
```

```
ops/shipments/metadata/v1.metadata.json
ops/shipments/metadata/v2.metadata.json
ops/shipments/metadata/version-hint.text
ops/shipments/metadata/snap-3259709588464726218-1-....avro
ops/shipments/metadata/....-m0.avro
ops/shipments/data/00000-0-....parquet
```

That is the whole chain, in order: `version-hint.text` names the current metadata JSON; the metadata JSON holds the schema and a list of snapshots; a snapshot points at a **manifest list** (`snap-*.avro`); the manifest list points at **manifests** (`*-m0.avro`); manifests point at **data files**. Every level carries statistics, and that is what lets a query skip work.

Reading those files by hand once is worth more than any explanation of them.


## Step 6 — check it

```bash
make warehouse                       # verified: creates both tables
make test                            # verified: 30 tests, no Docker
make run                             # verified: still prints "catalog none"
```

Not yet verified, once Docker is back:

```bash
make up
open http://localhost:9001           # MinIO console: the warehouse bucket
curl -s localhost:8181/v1/config     # the REST catalog answers
make bootstrap
```

Then confirm the tables landed in object storage rather than on a disk somewhere:

```bash
docker compose -f docker/compose.yaml run --rm minio-init \
  sh -c 'mc alias set local http://minio:9000 pipeline-local pipeline-local-secret &&
         mc ls -r local/warehouse'
```

And confirm the gate still holds — `make submit` should print `catalog none`, because PR 7 deliberately does not give the `submit` service any Iceberg environment. Wiring the runner is PR 8's job.


## Definition of done

- [ ] `make warehouse` creates both tables with no Docker running
- [ ] `make run` still reports `catalog none` and reaches no network
- [ ] `make bootstrap` creates the tables through the REST catalog into MinIO
- [ ] The MinIO console shows objects under `warehouse/ops/`
- [ ] `make test` passes, including the catalog-option assertions
- [ ] Every third-party image is pinned by digest

## Files in this PR

| File                                          |                                                                               |
| --------------------------------------------- | ----------------------------------------------------------------------------- |
| `src/pipeline/session.py`                     | new — `CatalogConfig`, `catalog_options`, and `build_session` moved here      |
| `src/pipeline/bootstrap.py`                   | new — namespace and tables from the declared schemas                          |
| `tests/unit/test_session.py`                  | new — seven tests, no JVM needed                                              |
| `src/pipeline/main.py`                        | builds from `session.py`, reports the catalog                                 |
| `tests/unit/test_main.py`                     | import moved                                                                  |
| `docker/compose.yaml`                         | MinIO (Silo), `minio-init`, the REST catalog, a `bootstrap` profile           |
| `Makefile`                                    | `bootstrap`, `warehouse`                                                      |
| `README.md`, `.gitignore`, `.dockerignore`    | the local warehouse directory                                                 |

Next: **PR 8** — `io/iceberg.py` and `runner/batch.py`: read the tables, run the join, write the result. Green against empty tables.
