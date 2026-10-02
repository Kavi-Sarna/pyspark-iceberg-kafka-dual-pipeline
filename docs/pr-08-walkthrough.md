# PR 8 — walkthrough

**Goal:** the first end-to-end run. Read two Iceberg tables, apply the core join, write the result back — and be green when the tables are empty.

**Why now:** PR 6 gave us a join with nothing to feed it and PR 7 gave us tables with nothing reading them. This is the seam, and it is deliberately the only thing in this PR so that a failure is unambiguous.

**Not in this PR:** partitioning, table properties, compaction, streaming.


## Step 0 — three layers, one direction

```
io/iceberg.py   ──read──►  core/join.py  ──►  io/iceberg.py   ──write──►
                            (pure)
        ▲                                              ▲
        └──────────── runner/batch.py ─────────────────┘
```

The rule that makes this worth separating: **`core/` may not import `io/`**, and `io/` may not contain domain logic. The runner is the only module that knows about both, and it is fifteen lines.

You can check the rule rather than trust it:

```bash
grep -rn "pipeline.io\|pipeline.runner" src/pipeline/core/   # no matches
```


## Step 1 — the adapter

```python
def read(spark: SparkSession, table: str) -> DataFrame:
    """Read a table by name.

    No schema is passed: Iceberg stores it, so the table is the declaration.
    That is the difference from reading files, where inference would guess.
    """
    return spark.table(table)
```

That docstring is the whole argument for a table format. PR 5 went to some trouble to declare schemas because reading *files* means inferring or hand-writing a schema at every read site. Reading a *table* means the schema came with it — the catalog is the single source of truth, and it is the one PR 7's bootstrap wrote from `StructType.toDDL()`.

Two write modes, and the difference matters:

```python
def append(df: DataFrame, table: str) -> None:
    """Add a snapshot. Existing data is untouched."""
    df.writeTo(table).append()


def overwrite(df: DataFrame, table: str) -> None:
    """Replace the table contents in one atomic snapshot.

    A reader either sees all the old rows or all the new ones, never a mixture,
    because the swap is a metadata pointer move.
    """
    df.writeTo(table).overwritePartitions()
```

Atomicity here is not a database feature bolted on: a commit is a new metadata JSON plus a pointer move, so there is no window where half the files are visible. That is the thing a directory of Parquet files cannot give you.


## Step 2 — the runner

```python
def run(spark: SparkSession, catalog: str) -> int:
    """Join the two tables and append the result. Returns the rows written."""
    shipments = read(spark, table_name(catalog, NAMESPACE, SHIPMENTS))
    deliveries = read(spark, table_name(catalog, NAMESPACE, DELIVERIES))

    joined = left_join(shipments, deliveries, SPEC)
    joined.cache()
    try:
        written = joined.count()
        append(joined, table_name(catalog, NAMESPACE, OUTPUT))
    finally:
        joined.unpersist()

    log.info("wrote   %d rows to %s", written, table_name(catalog, NAMESPACE, OUTPUT))
    return written
```

Three things worth pointing at.

**The spec lives here, not in the core.** Batch supplies `max_lag=None`; the streaming runner will supply a real bound, because a stream-stream left outer join will not plan without one. The core never learns which mode it is in.

**`cache()` around the count.** A DataFrame is a plan, not data. Calling `count()` and then `append()` would execute the join *twice* — reading both tables twice and, worse, computing the result twice. Caching makes the second action reuse the first. The `finally: unpersist()` is not decoration either: cached blocks sit in executor memory until released, and a long-lived driver that forgets will eventually spill or fail.

**It returns a number.** A runner that returns `None` can only be tested by inspecting the world afterwards. Returning the row count gives the tests something direct to assert, and gives the log something true to say.


## Step 3 — the mode flag

```python
def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="pipeline")
    parser.add_argument("--mode", choices=MODES, default="report")
    return parser.parse_args(argv)
```

`report` stays the default so `make run` behaves exactly as it has since PR 1 — no catalog, no network. `--mode batch` is opt-in, and it fails fast with a readable message rather than a stack trace when no catalog is configured:

```python
    if args.mode == "batch" and catalog is None:
        log.error("batch mode needs a catalog; set ICEBERG_WAREHOUSE")
        return 1
```

`argv` is a parameter rather than being read from `sys.argv` inside. This is not hypothetical tidiness — the unit test that calls `main()` started failing with `SystemExit: 2` the moment the flag existed, because argparse was reading *pytest's* arguments. Passing `main([])` fixes it, and an entry point you can call with explicit arguments is testable in a way one that reaches for global state is not.


## Step 4 — the packages gotcha, again but inverted

PR 7 established that `spark.jars.packages` works from the session builder, because PySpark starts the JVM at `getOrCreate()` and folds the setting into the launch.

**Under `spark-submit` that is no longer true.** The JVM is already running before your Python executes, so a builder-set `spark.jars.packages` is read too late and silently ignored. You get a `ClassNotFoundException` for `SparkCatalog` and nothing that mentions jars.

So the Compose service passes it on the submit command instead:

```yaml
        --packages "$$(/app/.venv/bin/python -c 'from pipeline.session import PACKAGES; print(PACKAGES)')"
```

The coordinates are asked of the code rather than copied into YAML, so there is still one definition. The rest of the catalog configuration — `spark.sql.extensions`, the `spark.sql.catalog.*` keys — is applied by the builder and works fine, because those are read when the *SparkSession* is created, which is our Python code, not when the JVM starts.

Worth internalising the general shape: **anything the JVM needs at launch must be on the launch command; anything the SparkSession needs can come from the builder.** Jars are the first category. Almost everything else is the second.


## Step 5 — credentials, and why environment variables were not enough

Running PR 7's bootstrap against real MinIO surfaced this: the AWS credentials had to be passed as Java options rather than environment variables.

The root cause is that `iceberg-aws-bundle` uses the AWS SDK's default credential chain, which reads the environment of *whichever JVM performs the I/O*. That is not reliably the process holding your variables — under spark-submit the driver is launched by a script, and executors are launched by the worker, which has its own environment entirely.

Passing them as Java options works, and it is a workaround. The fix is to hand them to `S3FileIO` directly as catalog properties, which travel with the catalog configuration to wherever the I/O happens:

```python
        if cfg.access_key and cfg.secret_key:
            options[f"{at}.s3.access-key-id"] = cfg.access_key
            options[f"{at}.s3.secret-access-key"] = cfg.secret_key
```

One caveat to be deliberate about: Spark configuration is visible in the driver UI's Environment tab. For a local stack with throwaway credentials that is fine. In anything real you would use an instance role or a web identity token and set neither of these.


## Step 6 — tests that actually touch Iceberg

The unit suite cannot test this path: there is no table without a catalog, and no catalog without the jars. So `tests/integration/` runs against a real `hadoop` catalog in a temp directory — no Docker, no object storage, just a directory.

**They must run in their own process**, and the fixture says so out loud:

```python
    if SparkSession.getActiveSession() is not None:
        pytest.fail(
            "a SparkSession already exists, so the Iceberg jars cannot be added. "
            "Run the integration tests in their own process."
        )
```

One JVM per process, `spark.jars.packages` read once at launch. If the unit tests have already built a plain session, `getOrCreate()` returns it without Iceberg and every test here fails with a classpath error that points nowhere near the cause. Hence:

```toml
testpaths = ["tests/unit"]
```

```bash
make test               # 36 unit tests, fast, no jars
make test-integration   # 7 integration tests, own process
```

and `ci/run.sh` invokes the two separately.

The tests are intentionally sequential, because the interesting properties are about accumulation:

| Test                                                      | Claim                                                                 |
| --------------------------------------------------------- | --------------------------------------------------------------------- |
| `test_a_run_against_empty_tables_is_green`                | Zero rows in, zero out, exit clean. The headline property.            |
| `test_a_populated_run_writes_the_join`                    | The F3 dataset through storage gives the same four rows as in memory. |
| `test_the_undelivered_shipment_survived_the_round_trip`   | `shp-2`'s null came back as a null.                                   |
| `test_each_run_adds_a_snapshot`                           | Running twice leaves two snapshots.                                   |
| `test_appending_twice_duplicates_rows`                    | Eight rows, not four. Append is not idempotent.                       |

That last one asserts behaviour you probably do not want in production, and it is there on purpose. Re-running this job doubles the output. Nothing in Iceberg prevents that, and discovering it from a row count that quietly drifted is worse than having a test say it in writing. Making the job a fixed point is PR 9's problem.


## Step 7 — run it

```bash
make test && make test-integration     # 36 + 7
make up
make batch                             # bootstrap, then submit to the cluster
```

Then go and look at what Iceberg recorded, which is the real exercise of this phase:

```sql
-- how many times has this job run?
SELECT committed_at, snapshot_id, summary['added-records']
FROM lake.ops.shipment_deliveries.snapshots ORDER BY committed_at;

-- what files exist, and what does each one know about itself?
SELECT file_path, record_count, file_size_in_bytes
FROM lake.ops.shipment_deliveries.files;

-- the manifest layer between snapshot and data file
SELECT path, added_data_files_count, existing_data_files_count
FROM lake.ops.shipment_deliveries.manifests;
```

```bash
# the same question, from the driver
docker compose -f docker/compose.yaml run --rm batch \
  /bin/bash -c '/app/.venv/bin/python -c "print(1)"'   # shell into the image

# and see the objects themselves
open http://localhost:9001                              # warehouse/ops/...
open http://localhost:18080                             # the run in the History Server
```

Run `make batch` a second time and watch `snapshots` grow by one and the row count double. That is time travel arriving for free, and duplication arriving with it.


## Definition of done

- [ ] `make test` and `make test-integration` both pass
- [ ] `make batch` writes to `lake.ops.shipment_deliveries` through MinIO
- [ ] A run against empty tables exits 0 and writes an empty snapshot
- [ ] `.snapshots` gains a row per run
- [ ] `make run` is unchanged: `report` mode, no catalog, no network
- [ ] Nothing under `core/` imports `io/` or `runner/`

## Files in this PR

| File                                      |                                                       |
| ----------------------------------------- | ----------------------------------------------------- |
| `src/pipeline/io/iceberg.py`              | new — `read`, `append`, `overwrite`, `table_name`     |
| `src/pipeline/runner/batch.py`            | new — the fifteen lines that assemble it              |
| `tests/integration/conftest.py`           | new — an Iceberg session on a temp directory          |
| `tests/integration/test_batch.py`         | new — seven tests through real tables                 |
| `tests/unit/test_batch_wiring.py`         | new — the parts that need no Spark                    |
| `src/pipeline/main.py`                    | `--mode`, and a readable failure without a catalog    |
| `src/pipeline/session.py`                 | S3 credentials as catalog properties                  |
| `src/pipeline/bootstrap.py`               | creates the output table too                          |
| `docker/compose.yaml`                     | a `batch` service that submits to the cluster         |
| `Makefile`, `ci/run.sh`, `pyproject.toml` | the unit/integration split                            |

Next: **PR 9** — partitioning and table properties: `PARTITIONED BY (days(...))`, `write.distribution-mode`, target file size, and making a re-run stop duplicating rows.
