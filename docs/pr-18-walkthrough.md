# PR 18 — walkthrough

**Goal:** six operational questions answered by running them, not by reasoning about them, and recorded so the answers stay answered.

**Why now:** everything is built. What is not yet known is how it behaves when something goes wrong — and the moment you need that knowledge is the worst possible moment to be acquiring it.

**Not in this PR:** anything new in the pipeline. This one only asks the pipeline questions.


## Step 0 — a drill is a test, or it is a story

The roadmap's instruction was *each drill gets three lines in the README: what you did, what happened, why*. The obvious way to do that is to run the drills by hand and write down the answers.

That produces a document which is true on the day it is written and silently false some time afterwards. Upgrade Spark, change the sink, alter the join, and the prose keeps claiming what used to happen.

So the drills are `tests/integration/test_drills.py`: each one performs the real operation against a real query or a real table and asserts the outcome. `docs/drills.md` is the prose, and it cannot drift far, because the thing it describes runs on every CI build.

```bash
make drills
```

One decision follows from that. The assertions check *relationships* — one row became two, this is smaller than that, this call raises — and not absolute numbers, because byte counts and timings move and a test that pins them is a test you learn to ignore.


## Step 1 — the sink decides what drill 2 is allowed to show

The first version of the checkpoint-deletion drill used the parquet sink, copied from PR 13's watermark harness. It reported **no duplicates**, which is the opposite of the expected answer, and the expected answer was right.

The parquet file sink keeps its own commit log, in a `_spark_metadata` directory beside the output. Given a fresh checkpoint the batch ids restart at zero, the sink looks them up, finds them already committed, and skips the write. Two independent mechanisms were being tested at once and the louder one won.

The Iceberg sink has no such log, and Iceberg is what the job actually writes to. Swapping it made the drill show what the job would really do.

The general form of that mistake is worth more than the specific one: **a drill measures the system you point it at**, and a harness built for one experiment carries assumptions that quietly invalidate another.


## Step 2 — restart (drill 1)

Run the query, let it finish, start it again unchanged.

Nothing is reprocessed. The checkpoint holds both halves of the query's position — the offsets already read, and the state already built — so a restart is a resume.

This is the drill that justifies decisions made three PRs earlier: the checkpoint is a named volume in Compose and a `PersistentVolumeClaim` on Kubernetes precisely so that a dead driver lands here rather than in drill 2.


## Step 3 — delete the checkpoint (drill 2)

Delete the checkpoint directory, leave the output table alone, restart.

Every row is read and written a second time. One row becomes two.

The sink has no memory of what it already holds. Spark re-reads from the beginning because nothing is left to say otherwise, and an Iceberg append does exactly what it is told.

The phrase to retire is "clear the checkpoint and let it rebuild". Deleting a checkpoint does not start clean. It emits everything again, into a table that already has it.


## Step 4 — change the query (drill 3)

Replace the two-source join with a single-source deduplication, point it at the existing checkpoint, and start.

```
assertion failed: There are [2] sources in the checkpoint offsets and now
there are [1] sources requested by the query. Cannot continue.
```

Spark refuses rather than guessing, which is the behaviour you want. The offset log records which sources the query had; a query with a different set of them is not the same query, and resuming would mean inventing a position for a source that was never read. The alternative to this exception is silent, wrong output.

In production the fix is a new checkpoint plus a deliberate decision about the gap or the overlap that creates — which is to say, drill 2, on purpose.


## Step 5 — evolve the Avro schema, and meet the five bytes again (drill 4)

Four cases, and the last two are the reason this drill is in the list.

| Writer                                    | Reader                        | Result                        |
| ----------------------------------------- | ----------------------------- | ----------------------------- |
| V2, optional field appended               | V1                            | reads fine, field ignored     |
| V2, **required** field appended           | V1                            | reads fine, field ignored     |
| V1                                        | V2, optional field appended   | **fails**                     |
| V2, field **inserted in the middle**      | V1                            | **wrong data, no error**      |

Avro's binary encoding carries no field names. It is positional, so a reader that wants two fields reads two fields and never looks at what follows — and that is why *writer ahead of reader* is safe whether the new field is optional or not. "Required" constrains the writer; it says nothing to a reader that stops before reaching it.

The third row is the one people get wrong, because an optional field with a default is supposed to be exactly the thing that makes this safe. It is — during **schema resolution**, which needs the writer's schema to resolve against:

```
Malformed records are detected in record parsing. Current parse Mode: FAILFAST.
```

And here the pipeline meets a decision it made in PR 12. `io/kafka.py`:

```python
# Magic byte plus a four-byte big-endian schema id.
HEADER_BYTES = 5
body = F.expr(f"substring(value, {HEADER_BYTES + 1}, length(value) - {HEADER_BYTES})")
return frame.select(from_avro(body, json.dumps(dict(schema))).alias("record"))
```

PR 12 stripped those five bytes because `from_avro` knows nothing about a registry, and treated the schema id as framing to be discarded. It is not framing. It is **the writer's identity**, and throwing it away means there is no resolution for a default to be applied during. The reader runs off the end of the record.

The fourth row came out of review, and it is the one worth carrying. "Writer ahead is safe" is true for *appending*, because a positional reader stops early. Insert the field in the middle instead and every field after it moves:

```python
# writer sent carrier="carrier-a", service_level="express"
{"shipment_id": "shp-1", "carrier": "express"}
```

No exception. `carrier` holds the service level, and plausible wrong rows go into the lake forever. An exception stops a pipeline; this does not.

So the proven-safe set is **appending, and only appending**. Reordering, removing a field, or changing a type are all unproven, and none of them can be detected by a decoder that never sees the writer's schema.

The consequence is the opposite of the usual advice: **append only, and deploy the producer first**. The usual advice assumes a consumer that looks the writer's schema up by that id. Making it true here means resolving the id against the registry instead of discarding it — a real piece of work, and now a known one rather than an assumption. Until then, the registry's compatibility rules are protecting a reader that is not using them.


## Step 6 — compaction does not free anything (drill 5)

Six single-row inserts, then the maintenance procedures.

```
6 inserts          files=6  snapshots=6  rows=6
rewrite_data_files rewritten=6 added=1
after compaction   files=1  snapshots=7  rows=6
  the first snapshot is still readable: 1 row
expire_snapshots   6 data files deleted
after expiry       files=1  snapshots=1  rows=6
  the first snapshot: Cannot find snapshot with ID ...
```

Six commits wrote six files, which is not a mistake — it is the shape of streaming, one file per trigger, forever.

Compaction merges them and **adds a snapshot**. It frees nothing: the older snapshots still point at the old files, which is why time travel to the first snapshot still returns its one row. A table that is only ever compacted grows.

`expire_snapshots` is what deletes them, and the price is paid in history. The snapshot that was readable a moment ago is gone. Compaction and expiry are two halves of one operation, and running only the first is a common way to be surprised by a storage bill.

One edge worth knowing before an incident rather than during one: `rollback_to_snapshot` only walks **backwards** — the target must be an ancestor of the current snapshot. Returning needs `set_current_snapshot`. Reaching for the wrong one while recovering is how a recovery becomes a second incident.


## Step 7 — RocksDB, measured with the instrument already in the box (drill 6)

Run the same stateful query under both state stores and read the state size out of query progress.

| Provider                                  | Reported state memory, 1,500 rows     |
| ----------------------------------------- | ------------------------------------- |
| `HDFSBackedStateStoreProvider` (default)  | 589,808 bytes                         |
| `RocksDBStateStoreProvider`               | 115,601 bytes                         |

The default keeps state in JVM hash maps on the executor heap, so a wide join window is a heap problem: it arrives as GC pressure and long pauses, which name the state store nowhere. RocksDB keeps state off-heap and spills to disk, trading per-trigger latency for a bound that does not depend on how much state accumulates.

The numbers are shape, not benchmark — the two providers count memory differently, and the gap on a real workload depends on the window and the key cardinality. What matters is which number grows with your state and which one does not.

The part worth noticing is *how* it was measured:

```python
from pipeline.metrics import metrics_from

...
metrics = metrics_from(json.loads(query.lastProgress.json))
return metrics["state.0.rows"], metrics["state.0.memory_bytes"]
```

That is PR 16's own extraction, reused. The observability built two PRs ago is the instrument the drill is run with, which is the only real test of whether it was worth building.


## Step 8 — what the finished thing is

Eighteen PRs, and the claim they were all in service of: **one artifact, two modes, one core**.

```
src/pipeline/core/      pure: schemas.py, join.py - imports no config, no io, no runner
src/pipeline/io/        adapters: iceberg.py, kafka.py
src/pipeline/runner/    assembly: batch.py, stream.py
src/pipeline/main.py    --mode batch | stream | report
```

`left_join` is the same function in both modes. What differs is a `JoinSpec` with a `max_lag` set or not set — a time bound the batch path does not need and the streaming path will not plan without. Not a branch, not a subclass: one field, passed as data.

Everything else is infrastructure arranged so that stays true.


## Definition of done

- [ ] `make drills` passes, naming each drill
- [ ] Every number quoted in `docs/drills.md` comes from an assertion
- [ ] The drills need no Docker and no broker
- [ ] Deleting a checkpoint demonstrably duplicates, against the real sink
- [ ] Changing the query demonstrably refuses, with the message quoted
- [ ] Drill 6 is measured with `metrics_from`, not a separate instrument
- [ ] Drill 4 calls `io.kafka.decode`, framing bytes and all, not a copy of it


## Files in this PR

| File                                  |                                                                               |
| ------------------------------------- | ----------------------------------------------------------------------------- |
| `tests/integration/test_drills.py`    | eleven tests, six drills, each performing the real operation                  |
| `docs/drills.md`                      | what was done, what happened, why - with the numbers those tests produce      |
| `Makefile`                            | `make drills`                                                                 |
| `pyproject.toml`                      | a mypy override for py4j, which names the exception a Spark procedure raises  |
| `README.md`                           | status, a Drills section, layout                                              |

That is PR 18 of 18. The roadmap is finished; the [Left Join Launchpad](https://gist.github.com/Kavi-Sarna/a06d702eb88946c7c4ab195a15529449) has the exercises that go beyond it.
