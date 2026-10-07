# PR 12 — walkthrough

**Goal:** read the two Kafka topics, decode the Avro, attach watermarks, run the same join, and write to the same Iceberg table — as a continuous query.

**Why now:** the topics exist and a producer fills them. This is where the second mode arrives, and the point of the whole design is that the join does not change.

**Not in this PR:** testcontainers, the state-store inspection, the one-artifact mode switch.


## Step 0 — the three rules of a stream-stream left join

Verified, in this order, with two streams from the `rate` source:

```
no watermarks, plain equality:
  Stream-stream LeftOuter join ... is not supported without a watermark

watermarks on both sides, still no time bound:
  Stream-stream LeftOuter join ... is not supported without a watermark

watermarks plus a time-range bound:
  PLANNED ok
```

The middle one is the result worth keeping. Most summaries say "you need watermarks", and that is half the rule. **Watermarks on both sides are necessary and not sufficient** — without a bound on how far apart the two event times may be, the engine cannot decide when a left row has waited long enough, so it refuses to plan at all.

That is the difference between batch and streaming, and the design handles it as data:

```python
SPEC = JoinSpec(
    keys=(JOIN_KEY,),
    left_time="dispatch_time",
    right_time="delivered_time",
    max_lag=timedelta(minutes=30),
)
```

`batch.SPEC` has `max_lag=None`; `stream.SPEC` sets it. `core/join.py` is untouched by this PR and does not know which mode it is in. A test asserts exactly that:

```python
def test_the_batch_spec_is_the_one_without_a_bound(iceberg: SparkSession) -> None:
    """The mode difference is data. Nothing branches on it."""
    assert batch.SPEC.max_lag is None
    assert stream.SPEC.max_lag is not None
    assert batch.SPEC.keys == stream.SPEC.keys
```

### Why the delay is the semantics

An unmatched shipment cannot be emitted when it arrives. The engine cannot distinguish "no delivery yet" from "no delivery ever" — the only thing that settles it is event time advancing far enough that no match could still be valid. Until then the row sits in state. That wait is not a bug to tune away; it is what a correct left outer join on an unbounded stream means.


## Step 1 — five bytes, and what happens if you forget them

The producer wrote a magic byte and a four-byte schema id in front of the Avro. `from_avro` takes a schema string and knows nothing about either, so the adapter strips them:

```python
def decode(frame: DataFrame, schema: Mapping[str, Any]) -> DataFrame:
    """Strip the Confluent framing, then read the Avro body.

    from_avro takes a schema string and knows nothing about a registry, so the
    prefix the producer wrote has to come off by hand. substring is 1-indexed.
    """
```

Now the part worth stopping for. Forgetting the strip **does not fail**:

```
Row(shipment_id='', carrier='', dispatch_time=1969-12-31 19:00)
```

Every string empty, the timestamp at the epoch. Avro reads the framing bytes as the beginning of the record and produces a well-formed row of nothing. A pipeline wired this way starts cleanly, writes rows at the expected rate, and every one of them is blank — which is far harder to notice than a crash.

It has its own test, asserting the corruption rather than an exception, because an exception is what I expected and not what happens.


## Step 2 — the nullability problem, which is not about Avro

The streaming write to Iceberg failed:

```
java.lang.IllegalArgumentException: Cannot write incompatible dataset to table with schema
```

The decoded columns come back nullable even though the Avro schema has no unions and the `StructType` declares `NOT NULL`. My first guess was that `from_avro` was at fault. It is not:

```python
def test_streaming_sources_are_always_nullable(iceberg, tmp_path) -> None:
    """Not an Avro quirk. A file source handed a non-nullable schema still
    reports every column nullable ..."""
    source = iceberg.readStream.schema(schema).parquet(...)
    assert [f.nullable for f in source.schema.fields] == [True, True]
```

A file source, given an explicitly non-nullable schema, also reports everything nullable. **Streaming schemas are nullable, full stop.** Iceberg then refuses to write them into required columns, which is correct of it.

Three ways out, and only one of them is honest:

|                                           |                                                                                           |
| ----------------------------------------- | ----------------------------------------------------------------------------------------- |
| Relax the table to optional columns       | Gives up the contract the batch path proves and other readers depend on.                  |
| `check-nullability=false` and move on     | Tells Iceberg the data is non-null when nothing checked. The lie moves into the table.    |
| Enforce it, then stop checking            | The claim becomes true before it is asserted.                                             |

So the adapter enforces it:

```python
    # Every streaming source reports its columns nullable, whatever schema it
    # was given, so the declared contract has to be enforced rather than
    # assumed. None of these events has an optional field, so any null means a
    # record that does not match its own schema.
    decoded = decode(raw, load_schema(schema)).dropna()
```

and the runner disables the check having earned it:

```python
    # Iceberg refuses a nullable dataset into a required column, and a stream
    # is always nullable. read_stream drops any row with a null, so the claim
    # this disables checking for is one we have already enforced. Disabling it
    # without that would just move the lie into the table.
    spark.conf.set("spark.sql.iceberg.check-nullability", "false")
```

The general shape is worth keeping: **a flag that turns off a safety check is only acceptable if you have replaced the check with something.**


## Step 3 — the checkpoint is not on MinIO, deliberately

The plan said a MinIO checkpoint location. It is a volume instead.

Structured Streaming checkpoints use rename for atomicity, and an object store has no rename — it has copy-then-delete, which is neither atomic nor cheap. It can be made to work, and plenty of production jobs do it, but it needs `hadoop-aws` plus an AWS SDK bundle (the Iceberg AWS bundle is shaded and does not satisfy S3A), which is roughly half a gigabyte of jars added to the image for a local demo.

For a laptop, a Docker volume is the honest stand-in for durable storage the driver owns. The location is a flag, so switching costs nothing once the jars are there:

```
--checkpoint /var/lib/pipeline/checkpoints/stream
```

Worth being clear what the checkpoint *is*: it holds the Kafka offsets already processed and the join's state store. Delete it and the query replays from `startingOffsets`; keep it and a restart resumes mid-stream. It is the only thing standing between a restart and either duplicate work or lost work.


## Step 4 — a bounded source, or the test never ends

The first version of the streaming test hung. `processAllAvailable()` on the `rate` source never returns, because a rate stream always has more available.

```python
    """availableNow bounds the source, so the query terminates. processAllAvailable
    on an unbounded source such as rate never returns."""
```

A file source with `.trigger(availableNow=True)` processes exactly what is present and stops, which is what a test needs. `rate` is fine for checking whether a plan is *accepted* — that needs no data to flow — and wrong for anything that has to finish.

Also: `.trigger()` takes exactly one argument. Passing `availableNow=False` alongside `processingTime` raises `ONLY_ALLOW_SINGLE_TRIGGER`, which is a clearer error than most.


## Step 5 — run it

```bash
make up
make stream AWAIT_SECONDS=90     # in one terminal
make produce SCENARIO=matched    # in another
make produce SCENARIO=unmatched
make produce SCENARIO=advance    # watch the unmatched row appear
```

Then look:

```bash
# rows arriving
docker compose -f docker/compose.yaml run --rm batch /bin/bash -c \
  '/app/.venv/bin/python -c "
from pipeline.session import CatalogConfig, build_session
s = build_session(\"q\", catalog=CatalogConfig.from_env())
s.table(\"lake.ops.shipment_deliveries\").show()"'

# the query itself
open http://localhost:4040/StreamingQuery/
```

The experiment worth doing slowly: send `unmatched`, confirm nothing appears, wait, confirm it still has not appeared, then send `advance` and watch the null-padded row arrive. The row was never lost — it was waiting for proof that no delivery could still match.


## Definition of done

- [ ] `make stream` starts and keeps running
- [ ] `make produce SCENARIO=matched` produces a joined row
- [ ] `unmatched` produces nothing until `advance` is sent
- [ ] Restarting the query resumes rather than replaying
- [ ] `core/join.py` is unchanged by this PR
- [ ] `make test` and `make test-integration` pass with no broker


## Files in this PR

| File                                  |                                                       |
| ------------------------------------- | ----------------------------------------------------- |
| `src/pipeline/io/kafka.py`            | new — strip, decode, enforce, watermark               |
| `src/pipeline/runner/stream.py`       | new — the streaming assembly                          |
| `tests/integration/test_stream.py`    | new — seven tests, no broker                          |
| `src/pipeline/main.py`                | `--mode stream`, `--checkpoint`, `--await-seconds`    |
| `src/pipeline/session.py`             | `extra_packages` and `STREAM_PACKAGES`                |
| `tests/integration/conftest.py`       | the Avro jar, which the wheel does not ship           |
| `docker/compose.yaml`, `Makefile`     | a `stream` service and target                         |
| `README.md`                           | status                                                |

Next: **PR 13** — testcontainers for Kafka and Schema Registry, and the watermark experiment as an assertion rather than a thing you watch.
