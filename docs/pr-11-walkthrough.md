# PR 11 — walkthrough

**Goal:** a producer you can reason about — fixed event times, explicit pauses, and named scenarios that mean the same thing every run.

**Why now:** the topics exist and nothing writes to them. The streaming work ahead is almost entirely about *when* one event arrives relative to another, so the input has to be controllable before any of it can be tested.

**Not in this PR:** `io/kafka.py`, the stream runner, watermarks. Nothing reads these messages yet.


## Step 0 — controllable, not fast

A load generator answers "how many per second". None of the questions ahead are that question. They are:

- A shipment arrives with no delivery. What is emitted, and when?
- Nothing happens for a while. Does the pending row appear?
- A later event arrives. *Now* does it appear?

Answering those needs two kinds of time held separately:

|                       |                                           |
| --------------------- | ----------------------------------------- |
| **event time**        | the timestamp inside the record. Fixed offsets from a constant base, so a scenario means the same thing today and next month.                 |
| **arrival time**      | when the record is sent. An explicit pause per step, so a watermark can be held still and then pushed on purpose.                              |

Conflating them is the usual mistake. A producer that stamps `datetime.now()` makes every assertion about watermarks unwritable, because the numbers move every run.

```python
BASE = datetime(2026, 9, 13, 10, 0, tzinfo=UTC)


def at(minutes: int) -> datetime:
    """Event time, as an offset from a fixed base rather than from now."""
    return BASE + timedelta(minutes=minutes)
```

A test pins this, because it is the property everything downstream rests on:

```python
def test_event_time_does_not_depend_on_when_you_run_it() -> None:
    assert at(0) == datetime(2026, 9, 13, 10, 0, tzinfo=UTC)
```


## Step 1 — the Confluent wire format, by hand

A Kafka message carrying Avro is not just Avro. Confluent prefixes five bytes:

```
+------+------------------+---------------------------+
| 0x00 | schema id, 4 BE  | Avro binary body          |
+------+------------------+---------------------------+
```

The magic byte is a format version. The id lets a consumer fetch *the schema this record was written with*, rather than assuming the subject's current version — which is the entire mechanism behind safe schema evolution.

```python
def encode(record: Mapping[str, Any], schema: Mapping[str, Any], registry_id: int) -> bytes:
    """Confluent wire format: magic byte, 4-byte big-endian id, Avro body.

    Written by hand because the consumer has to strip it by hand: Spark's
    from_avro takes a schema string and knows nothing about a registry.
    """
```

That docstring is the reason for the whole approach. Checked directly:

```
from_avro signature: (data, jsonFormatSchema: str, options=None)
```

No registry, no id, no framing. So PR 12 will have to drop five bytes before calling it. Writing the prefix by hand here keeps the pair symmetric and visible, instead of one side being magic and the other manual.

Three tests cover the bytes, including one that would otherwise be invisible:

```python
def test_the_schema_id_is_big_endian() -> None:
    """Confluent specifies network byte order. Little-endian would still be
    four bytes, decode to a plausible id, and fetch the wrong schema."""
    raw = encode(..., registry_id=258)  # 0x0102: byte order is visible
    assert raw[1:5] == b"\x00\x00\x01\x02"
```

Picking 258 rather than 1 is deliberate. With a small id both byte orders produce the same bytes and the test proves nothing.


## Step 2 — the scenarios

```python
SCENARIOS: Mapping[str, Sequence[Step]] = {
    "matched": [...],  # one shipment, one delivery
    "unmatched": [...],  # a shipment with no delivery, ever
    "advance": [...],  # a later shipment, to push event time forward
    "duplicate": [...],  # two deliveries for one shipment
    "out-of-order": [...],  # a delivery earlier than its shipment
    "watermark": [...],  # unmatched, a pause, then the release
}
```

Each one exists to answer a specific question later. `unmatched` and `advance` are a pair: the first produces a row the engine cannot emit, the second proves it was waiting rather than lost.

### Everything is keyed by `shipment_id`

```python
def test_every_record_is_keyed_by_shipment_id() -> None:
    """Same key, same partition, so a shipment and its deliveries are ordered
    with respect to each other rather than racing."""
```

Kafka orders within a partition and makes no promise across them. With three partitions and no key, a delivery could be read before its shipment and the join would behave differently run to run. Keying both sides on `shipment_id` puts them on the same partition, which turns their order from a race into a fact. The test enforces it for every step of every scenario, because one unkeyed record is enough to make a failure intermittent.


## Step 3 — run it

```bash
make up
make produce                          # matched
make produce SCENARIO=unmatched
make produce SCENARIO=watermark       # pauses 10s, then advances event time
make produce SCENARIO=duplicate SPEED=10   # same shape, pauses divided by 10
```

Then read the topic back, which is the only way to be sure anything landed:

```bash
docker compose -f docker/compose.yaml exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic ops.shipments --from-beginning --max-messages 1 \
  --property print.key=true
```

The value will look like binary with readable fragments — that is Avro, and the first five bytes are the framing. To see the framing itself:

```bash
docker compose -f docker/compose.yaml exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic ops.shipments \
  --from-beginning --max-messages 1 | xxd | head -2
```

Expect `00 00 00 00 01` or similar: magic byte, then the schema id the registry handed out.

Check offsets to confirm the keying actually spread records the way you expect:

```bash
docker compose -f docker/compose.yaml exec kafka \
  /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 --topic ops.shipments
```

All of one shipment's records will sit on one partition. Run `duplicate` and confirm its three records share a partition.


## Definition of done

- [ ] `make produce` sends and exits cleanly
- [ ] Every scenario runs, and `SPEED` shortens the pauses
- [ ] A consumed message starts with `0x00` and a four-byte id
- [ ] All of a shipment's records land on one partition
- [ ] Event times are the same on two runs a day apart
- [ ] `make test` passes with no broker running


## Files in this PR

| File                              |                                               |
| --------------------------------- | --------------------------------------------- |
| `src/pipeline/producer.py`        | new — scenarios, the wire format, the CLI     |
| `tests/unit/test_producer.py`     | new — twelve tests, no broker                 |
| `src/pipeline/streams.py`         | `schema_id()`                                 |
| `docker/compose.yaml`, `Makefile` | a `produce` service and target                |
| `pyproject.toml`, `uv.lock`       | `fastavro`                                    |
| `README.md`                       | status                                        |

Next: **PR 12** — `io/kafka.py` and `runner/stream.py`: strip those five bytes, decode the Avro, attach watermarks, and run the join as a stream.
