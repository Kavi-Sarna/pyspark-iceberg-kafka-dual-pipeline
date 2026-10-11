# Operational drills

Six things worth having done before you have to do them. Each is a real operation against a real query or a real table, run by `tests/integration/test_drills.py`, so the answers below cannot quietly stop being true:

```bash
make drills
```

Numbers are from a run on a directory catalog with file sources. The tests assert the relationships, not the byte counts, because those move.


## 1. Kill the driver mid-stream, then restart it

**Did.** Ran the query to completion, then started it again, unchanged, against the same checkpoint.

**Happened.** Nothing was reprocessed. One row in the table before, one row after.

**Why.** The checkpoint holds both halves of the query's position: the offsets already read, and the state already built. A restart is a resume. This is also why the checkpoint is a volume in Compose and a `PersistentVolumeClaim` on Kubernetes — if it dies with the pod, every restart is drill 2.

## 2. Delete the checkpoint and restart

**Did.** Deleted the checkpoint directory, left the output table alone, and started the query again.

**Happened.** Every row was read and written a second time. One row became two.

**Why.** The sink has no memory of what it already holds. Spark re-read the sources from the beginning because there was nothing left to say it had read them, and an Iceberg append does what it is told. Deleting a checkpoint is not "starting clean", it is "emitting everything again".

Worth knowing: a **parquet** sink hides this. It keeps its own `_spark_metadata` commit log and skips a batch id it has already written, so the same drill against parquet shows no duplicates at all. The drill uses the Iceberg sink because that is what the job writes to.

## 3. Change the query and restart on the old checkpoint

**Did.** Replaced the two-source join with a single-source deduplication and pointed it at the existing checkpoint.

**Happened.** Refused, with a `StreamingQueryException`:

```
assertion failed: There are [2] sources in the checkpoint offsets and now
there are [1] sources requested by the query. Cannot continue.
```

**Why.** The offset log records which sources the query had. A query with a different set of them is not the same query, and resuming would mean inventing a position for a source that was never read. Spark refuses rather than guessing, which is the behaviour you want: the alternative is silent, wrong output. The fix in production is a new checkpoint and a deliberate decision about the gap or the overlap that creates.

## 4. Evolve the Avro schema

**Did.** Added a field to the writer and read with the old reader; then the reverse; with an optional field and a required one; appended, and then inserted in the middle.

**Happened.**

| Writer                                    | Reader                        | Result                        |
| ----------------------------------------- | ----------------------------- | ----------------------------- |
| V2, field **appended** (optional)         | V1                            | reads fine, new field ignored |
| V2, field **appended** (required)         | V1                            | reads fine, new field ignored |
| V1                                        | V2, optional field appended   | **fails loudly**              |
| V2, field **inserted in the middle**      | V1                            | **wrong data, no error**      |

The third case fails like this:

```
Malformed records are detected in record parsing. Current parse Mode: FAILFAST.
```

The fourth does not fail at all. The writer sent `carrier="carrier-a", service_level="express"`; the old reader returned:

```python
{"shipment_id": "shp-1", "carrier": "express"}
```

**Why.** Avro's binary encoding carries no field names — it is positional. A reader that wants two fields reads two fields and never looks at what follows, which is why *writer ahead of reader* is safe whether the new field is optional or not. "Required" is a constraint on the writer; it says nothing to a reader that stops before reaching it.

The third row is the one that surprises people, because an optional field with a default is supposed to be exactly what makes this safe. It is — during **schema resolution**, which needs the writer's schema to resolve against. `io/kafka.py` strips the Confluent header and hands `from_avro` a fixed schema from a `.avsc` file, so the writer's schema id is discarded and there is no resolution to apply a default during. The reader simply runs off the end of the record.

And the fourth case is why "writer ahead is safe" is a narrower claim than it sounds. Appending is safe because a positional reader stops early. Inserting moves every field after it, and the reader has no way to know — so `carrier` quietly holds the service level. That is worse than the third case in every way that matters: an exception stops a pipeline, and this writes plausible rows into the lake forever.

**What is actually proven safe here is appending, and only appending.**
Reordering fields, removing one, or changing a type are all unproven by this drill and none of them can be detected by a decoder that never sees the writer's schema.

The consequence for this pipeline: **append only, and deploy the producer first.** The second half is the opposite of the usual advice, which assumes a consumer that looks the writer's schema up by the id in those five bytes. Making that true here means resolving the id against the registry rather than throwing it away — and until that happens, the registry's compatibility rules are protecting a reader that is not using them.

## 5. Compact an Iceberg table, then expire its snapshots

**Did.** Six single-row inserts, then `rewrite_data_files`, then time travel, rollback, and `expire_snapshots`.

**Happened.**

```
6 inserts          files=6  snapshots=6  rows=6
rewrite_data_files rewritten=6 added=1
after compaction   files=1  snapshots=7  rows=6
  the first snapshot is still readable: 1 row
expire_snapshots   6 data files deleted
after expiry       files=1  snapshots=1  rows=6
  the first snapshot: Cannot find snapshot with ID ...
```

**Why.** Six commits wrote six files, which is exactly what a streaming sink does at every trigger — the small-file problem is not a mistake, it is the shape of streaming. Compaction merges them and **adds a snapshot**; it frees nothing, because older snapshots still point at the old files and time travel still works. `expire_snapshots` is what deletes them, and the price is the history: the snapshot that was readable a moment ago is gone. A table that is only ever compacted grows forever.

Rollback has an edge worth knowing before an incident: `rollback_to_snapshot` only walks **backwards**, since the target must be an ancestor of the current snapshot. Going forward again needs `set_current_snapshot`. Reaching for the wrong one while recovering is how a recovery becomes a second incident.

## 6. Swap the state store to RocksDB

**Did.** Ran the same stateful query under both providers and read the state size out of query progress — using `metrics_from`, the same extraction the listener uses.

**Happened.** For 1,500 rows of state:

| Provider                                  | Reported state memory     |
| ----------------------------------------- | ------------------------- |
| `HDFSBackedStateStoreProvider` (default)  | 589,808 bytes             |
| `RocksDBStateStoreProvider`               | 115,601 bytes             |

**Why.** The default keeps state in JVM hash maps on the executor heap, so a wide join window is a heap problem: it shows up as GC pressure and long pauses before it shows up as anything that names the state store. RocksDB keeps state off-heap and spills to disk, trading some per-trigger latency for a bound that does not depend on how much state you accumulate.

Treat the figures as shape rather than as a benchmark — the two providers count memory differently, and the gap on a real workload depends on the window and the key cardinality. The point is which number grows with your state and which one does not.


## Running them

```bash
make drills            # all six, named
make test-integration  # the drills plus everything else
```

They need no Docker: file sources and a directory catalog, because the behaviour belongs to Spark and to Iceberg rather than to a broker.
