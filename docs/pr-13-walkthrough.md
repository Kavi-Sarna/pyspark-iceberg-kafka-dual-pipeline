# PR 13 — walkthrough

**Goal:** turn the streaming claims into assertions. The unmatched shipment that waits and then appears, the state store you can look inside, and a real broker for the bytes.

**Why now:** PR 12 made both modes run. Everything interesting about the streaming one is a claim about timing, and a claim about timing that nothing checks will quietly stop being true.

**Not in this PR:** the mode switch, Kubernetes, operational drills.


## Step 0 — two kinds of test, because two kinds of bug

|                       |                                                                                       |
| --------------------- | ------------------------------------------------------------------------------------- |
| `test_watermark.py`   | What the join *means*. File sources, no Docker, deterministic.                        |
| `test_kafka_wire.py`  | Whether the bytes survive a real broker. testcontainers, skipped without Docker.      |

Keeping them apart is the point. A semantics bug and a serialization bug look nothing alike, and finding both with one test means every failure needs investigating from scratch. Splitting them means a red test is already half a diagnosis.

It also means the semantics tests run everywhere, every time. They are the ones most likely to catch a regression, and tying them to a Docker daemon would have made them the ones most likely to be skipped.


## Step 1 — the experiment

```python
def test_advancing_event_time_releases_it(harness: Harness) -> None:
    harness.shipments(("shp-2", "carrier-b", at(5)))
    harness.advance()
    assert harness.emitted() == []

    harness.shipments(("shp-9", "carrier-z", at(300)))
    harness.deliveries(("dlv-9", "shp-9", at(301)))
    harness.advance()

    assert harness.emitted() == [("shp-2", None), ("shp-9", "dlv-9")]
```

Round one emits **nothing**. The shipment is not lost and not dropped; the engine cannot distinguish "no delivery yet" from "no delivery ever", so it holds the row. Round two adds an event far enough ahead that no delivery could still match, and `shp-2` comes out null-padded.

*Verified:*

```
round 1 (unmatched only):   []
round 2 (after advancing):  [('shp-2', None), ('shp-9', 'dlv-9')]
```

That delay is the semantics. Any system that emitted `shp-2` immediately would be guessing, and would have to retract the row if a delivery turned up.

### The companion test, which is the one that catches real outages

```python
def test_advancing_only_one_side_releases_nothing(harness: Harness) -> None:
    """A stream-stream join watermarks on the minimum across its inputs, so a
    quiet delivery topic pins the whole join no matter how far ahead the
    shipments run. This is the usual reason a streaming join looks broken."""
```

Shipments race ahead by five hours, deliveries stay silent, and **nothing is emitted**. The global watermark is the minimum across inputs, so one idle stream holds everything. This is the failure that looks like a broken join and is actually a quiet topic, and it is worth having in writing.


## Step 2 — two harness details that cost me a run each

**The memory sink cannot recover from a checkpoint.**

```
AnalysisException: This query does not support recovering from checkpoint location.
```

Round two has to *continue* the same query rather than start a new one, which means the sink has to support recovery. The parquet sink does. The harness uses it and reads the directory back.

**An empty seed, not a seeded row.**

The directories have to exist before a file source will start, so the harness creates them. The first version wrote one real shipment to do it — which was itself an unmatched shipment, and duly turned up in every assertion:

```
assert [('shp-0', None), ('shp-2', None), ('shp-9', 'dlv-9')] == [('shp-2', None), ...]
```

Empty DataFrames create the directories without adding data. Worth the care: test fixtures that quietly contribute rows are hard to spot because the test *looks* like it is only about what it writes.


## Step 3 — looking inside the state store

Spark 4 exposes streaming state as a readable source, which turns "the row is probably waiting in state" from an inference into something you can select:

```python
    left = (
        harness.spark.read.format("statestore")
        .option("path", str(harness.root / "ck"))
        .option("joinSide", "left")
        .load()
    )
    waiting = {r["key"]["field0"] for r in left.collect()}

    assert "shp-2" in waiting
```

*Verified.* The rows come back as key/value pairs:

```
left:  {'key': Row(field0='shp-2'), 'value': Row(shipment_id='shp-2', carrier='carrier-b',
                                                 dispatch_time=2026-09-13 10:50), 'partition_id': 1}
       {'key': Row(field0='shp-1'), ...}
right: {'key': Row(field0='shp-1'), 'value': Row(delivery_id='dlv-1', ...), 'partition_id': 1}
```

`joinSide` is the option that makes it useful for a join: `left` and `right` are separate stores, and seeing a key in one and not the other is exactly the picture of a half-finished join.

This is also the answer to "why is my streaming job using so much memory". The state store holds every unmatched row for as long as the lateness setting allows, so `LATENESS = "10 minutes"` is a memory decision as much as a correctness one, and now it is one you can measure rather than guess at.


## Step 4 — a real broker, for the bytes only

```python
@pytest.fixture(scope="module")
def broker() -> Iterator[str]:
    """A single-broker Kafka, torn down with the module."""
    from testcontainers.community.kafka import KafkaContainer

    with KafkaContainer() as container:
        yield container.get_bootstrap_server()
```

Note the import path. `testcontainers.kafka` still works and emits a `DeprecationWarning`; `testcontainers.community.kafka` is the current one. Worth checking rather than copying from an older example.

The fixture is **module-scoped**: starting a broker takes seconds, and doing it per test would make the file slow enough to skip.

These tests check what in-memory encoding tests cannot: that bytes written by our producer and read back through a real broker still decode. The producer and the adapter agreeing with each other proves nothing if both are wrong the same way.

```python
def test_the_first_five_bytes_are_the_framing(...)
    assert value[0] == 0
    assert int.from_bytes(value[1:5], "big") == 258
```

Reading the raw value off the topic rather than trusting the decoder, with a schema id chosen so byte order is visible.

### Skipping, rather than failing, without Docker

```python
pytestmark = pytest.mark.skipif(
    not _docker_is_up(), reason="no Docker daemon; these need a real broker"
)
```

A ping, not an import check: `testcontainers` imports perfectly well with no daemon running and then hangs trying to start a container. *Verified:* 24 passed, 5 skipped, on a machine with no daemon.


## Step 5 — check it

```bash
make test-integration          # 24 pass, 5 skip without Docker
```

With a daemon running, the five Kafka tests run too and the first one takes a while as the image is pulled.

Then break things deliberately:

```bash
# in io/kafka.py, change HEADER_BYTES to 4
make test-integration   # the round-trip test fails

# in runner/stream.py, remove max_lag from SPEC
make test-integration   # the join no longer plans

# in producer.py, make advance send only a shipment
make test-integration   # test_advancing_event_time_releases_it fails
```

That last one is the regression this PR exists to prevent.


## Step 6 — watch it rather than assert it

The tests assert silently. `make watermark-demo` runs the same experiment and prints each stage, with no Docker and no Kafka:

```
lateness 10 minutes, bound 0:30:00

--- a shipment with no delivery ---
  emitted     : nothing
  state left  : ['shp-2']
  state right : empty

--- after advancing event time on both topics ---
  emitted     : [('shp-2', None), ('shp-9', 'dlv-9')]
  state left  : ['shp-9']
  state right : ['shp-9']
```

The second block is the part worth sitting with. `shp-2` came out null-padded **and left the state store**; what remains is `shp-9`, still inside its window on both sides. Rows entering state, waiting, and leaving is the whole mechanism in six lines.

It imports `stream.LATENESS` and `stream.SPEC` rather than redeclaring them, so a change to the runner shows up here immediately.


## Definition of done

- [ ] The unmatched shipment is absent in round one and present in round two
- [ ] Advancing one side alone releases nothing
- [ ] The waiting shipment is visible in the state store by name
- [ ] A record round-trips through a real broker
- [ ] The Kafka tests skip, not fail, with no daemon
- [ ] `make ci` green


## Files in this PR

| File                                      |                                                       |
| ----------------------------------------- | ----------------------------------------------------- |
| `tests/integration/test_watermark.py`     | new — six tests, the experiment and the state store   |
| `tests/integration/test_kafka_wire.py`    | new — five tests against a real broker                |
| `pyproject.toml`, `uv.lock`               | `testcontainers[kafka]`, dev only                     |
| `README.md`                               | status                                                |

Next: **PR 14** — one artifact, two modes: the image that runs either runner from a flag, so batch and stream stop being separate deployments.
