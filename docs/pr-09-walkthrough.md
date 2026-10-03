# PR 9 — walkthrough

**Goal:** partition the tables, set the properties that control how files get written, and make a re-run stop duplicating its own output.

**Why now:** the pipeline works. This is the PR where it stops being a toy — the three things here are what separate a table you can query from a directory of Parquet you have to scan.

**Not in this PR:** compaction, snapshot expiry, streaming.


## Step 0 — hidden partitioning

Partition on a day:

```sql
CREATE TABLE IF NOT EXISTS lake.ops.shipments (...)
USING iceberg
PARTITIONED BY (days(dispatch_time))
```

Now look at the table:

```
output columns: ['shipment_id', 'carrier', 'dispatch_time', 'delivery_id', 'delivered_time']
```

**No `dispatch_time_day`.** That is hidden partitioning, and it is the headline feature of the table format.

In a Hive-style table, partitioning by day means a real column. Every writer has to derive and populate it, every reader has to know it exists and filter on it, and a reader who filters on `dispatch_time` alone gets a full scan with no warning. The derived column and the source column can disagree, and nothing checks.

Iceberg stores the transform in the partition spec instead. The column is a property of the *table*, not of the rows, so a query filtering on `dispatch_time` prunes automatically. You cannot forget to use it, and you cannot populate it wrong.

The partition still exists, it just lives in the metadata:

```sql
SELECT partition FROM lake.ops.shipment_deliveries.partitions;
-- {dispatch_time_day: 2026-09-13}
```

### Which column to partition on

The output is partitioned by `dispatch_time`, not `delivered_time`, and this is not arbitrary. `delivered_time` is null for exactly the rows this job exists to find. Partition on it and every undelivered shipment lands in a single null partition, which is both the worst-clustered and most-queried part of the table. **Partition on the column that is never null.**


## Step 1 — what partitioning actually buys, measured

The usual claim is "partitioning makes queries faster". That is true often enough to be misleading, and the first measurement I took said otherwise.

Ten days of shipments, one row per day, written to a partitioned and an unpartitioned table. Then a one-day filter:

```
part: planner produced 1 scan task
flat: planner produced 1 scan task
```

**Identical.** Iceberg keeps per-file column bounds in the manifests, so the unpartitioned table skipped nine of its ten files on min/max statistics alone. Partitioning added nothing, because the data happened to arrive already clustered by day — one file per day, written in order.

Now the same test on data that did *not* arrive sorted. A thousand rows across ten days, interleaved, coalesced into a single file before writing:

| table             | files on disk | rows read for a one-day filter    |
| ----------------- | ------------- | --------------------------------- |
| unpartitioned     | 1             | **1000**                          |
| partitioned       | 10            | **100**                           |

Ten times less data read, and the mechanism is not what the slogan suggests. The unpartitioned table has one file whose `dispatch_time` bounds span the whole range, so its statistics cannot exclude it and the whole file is read. The partitioned table *could not* write that file: the partition spec forced a split by day.

**Partitioning's real job is to guarantee clustering.** File statistics do the pruning either way; partitioning is what makes sure the statistics are worth consulting. Data that arrives sorted prunes fine without it. Data that does not — which is most real data, and all streaming data — does not.

That test is in the suite, asserting 1000 and 100, so the claim stays honest.


## Step 2 — the properties

```python
PROPERTIES: Mapping[str, str] = {
    "write.distribution-mode": "hash",
    "write.target-file-size-bytes": "134217728",
    "write.parquet.compression-codec": "zstd",
}
```

**`write.distribution-mode = hash`** is the one that changes output shape most. Without it, every Spark task writes its own file into every partition it touches. A job with the default 200 shuffle partitions writing three days produces up to 600 files, most of them tiny. `hash` shuffles by partition key first, so each day gets one writer and one file. The small-files problem is easier to avoid here than to fix later with compaction.

**`write.target-file-size-bytes = 134217728`** — 128 MB. Where writers roll over to a new file, and what a later `rewrite_data_files` aims for. Too small and you pay per-file metadata and open costs; too large and pruning gets coarser, because a file is the smallest thing statistics can exclude.

**`write.parquet.compression-codec = zstd`** — better ratio than snappy at similar speed, and the direction the ecosystem has moved.

Iceberg does **not** validate property names. Misspell one and it is accepted, stored, and silently does nothing. So the generated statement is a pure function with a test pinning it exactly:

```python
def create_table_sql(fqn: str, columns: str, spec: TableSpec) -> str:
    """The CREATE TABLE statement, as a string so it can be tested."""
```

and a second test reads the properties back off the created table, because "the statement was right" and "the property stuck" are different claims.


## Step 3 — the re-run stops duplicating

PR 8 left this deliberately broken, with a test saying so: running the job twice left eight rows where four were wanted. Append is not idempotent and Iceberg will not stop you.

One word fixes it:

```python
        # Replaces only the days this run produced, so re-running is a fixed
        # point rather than a doubling. Days absent from the input are left
        # alone, which is why the output is partitioned at all.
        overwrite(joined, table_name(catalog, NAMESPACE, OUTPUT))
```

`overwritePartitions()` is *dynamic* partition overwrite: it replaces the partitions present in the incoming data and leaves every other partition untouched. Re-running over the same day replaces that day. Running tomorrow adds tomorrow without disturbing today.

This is why Step 0 had to come first. On an unpartitioned table there is only one partition, so `overwritePartitions` would replace the entire table — turning a backfill of one day into the deletion of all the others.

*Verified.* Three runs:

```
run 1: wrote 4 rows
run 2: wrote 4 rows
run 3: wrote 4 rows

output rows : 4
snapshots   : 3
```

Four rows, three snapshots. The data is a fixed point and the history is not — every run is still recorded, still auditable, still time-travellable. That is the combination you want and the one a plain directory of files cannot give you.

### What it does not do

`overwritePartitions` on an empty result overwrites nothing. If a day's input disappears, the output for that day stays as it was rather than becoming empty. That is usually the behaviour you want from a backfill and occasionally not, and the only way to know which is to decide deliberately.


## Step 4 — the metadata chain, end to end

With partitioning in place, the chain the roadmap asks you to understand is visible at every level. Run `make local`, then:

```sql
-- catalog pointer -> metadata JSON -> snapshot
SELECT snapshot_id, committed_at, summary['added-records'], summary['deleted-records']
FROM lake.ops.shipment_deliveries.snapshots ORDER BY committed_at;

-- snapshot -> manifest list -> manifests
SELECT path, added_data_files_count, existing_data_files_count, deleted_data_files_count
FROM lake.ops.shipment_deliveries.manifests;

-- manifests -> data files, with the statistics that drive pruning
SELECT file_path, record_count, file_size_in_bytes, partition
FROM lake.ops.shipment_deliveries.files;

-- the partition layer that only exists in metadata
SELECT partition, record_count, file_count
FROM lake.ops.shipment_deliveries.partitions;
```

The four tiers of elimination, cheapest first: the **manifest list** excludes whole partitions, **manifests** exclude individual files, the **Parquet footer** excludes row groups, and the **page index** excludes pages. Partitioning only affects the first. Everything else was already working, which is exactly why the first measurement in Step 1 showed no difference.


## Step 5 — the thing to know before you re-run bootstrap

`CREATE TABLE IF NOT EXISTS` does nothing to a table that already exists. If you ran the bootstrap before this PR, your tables are **still unpartitioned** and `make bootstrap` will not change them.

For a local stack, start over:

```bash
make clean && make local                 # directory warehouse
make down && make up && make bootstrap   # Compose stack
```

For a table you cannot drop, Iceberg supports partition evolution:

```sql
ALTER TABLE lake.ops.shipment_deliveries ADD PARTITION FIELD days(dispatch_time);
```

Existing data files keep their old spec and new writes use the new one; Iceberg plans across both. That is a genuinely hard problem solved well, and worth knowing exists — but it is not what the bootstrap does, and quietly assuming otherwise is how you end up measuring a partitioned table that is not.


## Step 6 — check it

```bash
make clean && make local
make local                       # still 4 rows, now 2 snapshots
make test && make test-integration
```

Then confirm the claims rather than trusting them:

```bash
# hidden partitioning: no extra column
# partition pruning: 100 rows vs 1000
make test-integration -- -k "partition"
```

And break them deliberately:

```bash
# remove PARTITIONED BY from TableSpec and re-run
make clean && make test-integration   # the pruning test must fail at 1000 == 100

# change overwrite back to append in runner/batch.py
make clean && make local && make local
# the fixed-point test must fail: 8 rows, not 4
```


## Definition of done

- [ ] Tables are partitioned by day and carry the three properties
- [ ] No partition column appears in any table's schema
- [ ] Running the job twice leaves the same rows and one more snapshot
- [ ] The pruning test measures 100 against 1000
- [ ] Properties read back off the created table, not just off the statement
- [ ] `make ci` green

## Files in this PR

| File                                  |                                                                                   |
| ------------------------------------- | --------------------------------------------------------------------------------- |
| `src/pipeline/bootstrap.py`           | `TableSpec`, `PROPERTIES`, and `create_table_sql` as a testable string            |
| `src/pipeline/runner/batch.py`        | `append` becomes `overwrite` — the fixed point                                    |
| `tests/unit/test_bootstrap.py`        | new — the statement, and the non-null partition rule                              |
| `tests/integration/test_batch.py`     | the duplication test becomes a fixed-point test, plus partitioning and pruning    |
| `README.md`                           | status                                                                            |

Next: **PR 10** — Kafka in Compose, and the first streaming read.
