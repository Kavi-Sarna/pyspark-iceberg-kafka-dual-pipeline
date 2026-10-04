# PR 10 — walkthrough

**Goal:** a Kafka broker and a Schema Registry in the local stack, with the topics and the Avro subjects created by the bootstrap.

**Why now:** the batch path is finished. Streaming needs somewhere for events to arrive before anything can read them, and standing that up is a separate problem from consuming it.

**Not in this PR:** the producer, `io/kafka.py`, the stream runner, watermarks. Nothing reads or writes a topic yet.


## Step 0 — one broker, no ZooKeeper

```yaml
  kafka:
    image: apache/kafka:4.2.2@sha256:1213eb3943d551e5ed...
    environment:
      KAFKA_NODE_ID: "1"
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
```

KRaft: Kafka manages its own metadata quorum, so there is no ZooKeeper to run. Every tutorial older than about 2023 will tell you otherwise. One node playing both `broker` and `controller` is fine on a laptop and nowhere else.

The image is `apache/kafka`, the Apache one, rather than a vendor rebuild. Of the available tags `4.2.2` is the most recently published stable — note that higher version numbers exist with older publish dates, because patch releases land on several lines at once, so "newest tag" and "highest number" are different questions.

### Replication factor one, said three times

```yaml
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "1"
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "1"
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "1"
```

Kafka's internal topics default to three replicas. A single broker cannot provide them, so without these the broker starts, accepts your connection, and then fails the first thing that needs `__consumer_offsets` — which is not where you will look.


## Step 1 — the two-listener problem

This is the one that costs people an afternoon.

```yaml
      KAFKA_LISTENERS: INTERNAL://:9092,CONTROLLER://:9093,HOST://:29092
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:9092,HOST://localhost:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,HOST:PLAINTEXT
```

A Kafka client does not keep talking to the address you gave it. It connects once, asks for metadata, and the broker replies with the addresses to use for each partition — the **advertised** listeners. From then on the client dials those.

So the advertised address has to be reachable *from the client*, and a container and your laptop disagree about what reaches the broker:

| client                            | reaches the broker as |
| --------------------------------- | --------------------- |
| another container                 | `kafka:9092`          |
| your shell, `kcat`, a notebook    | `localhost:29092`     |

One listener can serve one of those and will fail for the other, after a successful initial connection, with a timeout naming a host the client could never resolve. Two listeners, each advertising what its audience can reach, is the fix.


## Step 2 — Schema Registry, and a port collision

```yaml
  schema-registry:
    image: confluentinc/cp-schema-registry:7.9.10@sha256:ae63c314e3f7...
    ports:
      - "8085:8081"
```

**8085 on the host, because 8081 is already the Spark worker UI.** Inside the network it stays 8081, which is what every Confluent example assumes, so nothing else has to change. Worth checking the port map whenever you add a service: a collision shows up as one of the two UIs mysteriously being the other one.

The registry stores its schemas in a Kafka topic, `_schemas`, which is why it waits on the broker being healthy and why it needs its own replication factor set to one.


## Step 3 — the Avro schemas, and where they live

```json
{
  "type": "record",
  "name": "Shipment",
  "namespace": "pipeline.ops",
  "fields": [
    {"name": "shipment_id", "type": "string"},
    {"name": "carrier", "type": "string"},
    {
      "name": "dispatch_time",
      "type": {"type": "long", "logicalType": "timestamp-millis"},
      "doc": "Event time. The watermark column on this side of the join."
    }
  ]
}
```

**The logical type is the whole point of that third field.** On the wire it is a `long`. Without `logicalType: timestamp-millis` every consumer sees an integer and has to be told, out of band, that it means a time. With it, Spark reads a timestamp and the watermark has something to be based on.

### They live inside the package

The obvious home is a top-level `schemas/` directory. That breaks in the image.

The runtime stage installs the project with `--no-editable`, so only `site-packages` is copied — a directory at the repo root is simply not there. The files live at `src/pipeline/avro/` and are read through `importlib.resources`, so they travel with the package wherever it is installed.

*Verified* against a non-editable install, which is what the image builds:

```bash
UV_PROJECT_ENVIRONMENT=/tmp/v uv sync --frozen --no-dev --no-editable
ls /tmp/v/lib/python3.12/site-packages/pipeline/avro/
# delivery.avsc  shipment.avsc
```

### Two declarations of one event

There are now two descriptions of a shipment: the Avro schema, which is the contract on the wire, and the `StructType`, which is the contract inside the job. Both are needed — Avro has logical types and namespaces that `StructType` cannot express, and `StructType` carries the nullability Spark enforces — but nothing stops them drifting apart.

So a test compares them:

```python
@pytest.mark.parametrize(("avsc", "struct"), [("shipment", SHIPMENT), ("delivery", DELIVERY)])
def test_avro_matches_the_spark_schema(avsc: str, struct: object) -> None:
```

*Verified that it bites.* Adding a field to the Avro schema alone:

```
FAILED tests/unit/test_streams.py::test_avro_matches_the_spark_schema[shipment-struct0]
```

Which is the only reason to keep two declarations at all. If the test did not exist you would be maintaining a lie.


## Step 4 — topics and subjects

```python
TOPICS: Mapping[str, TopicSpec] = {
    SHIPMENTS: TopicSpec(partitions=3, schema="shipment"),
    DELIVERIES: TopicSpec(partitions=3, schema="delivery"),
}
REPLICATION = 1
```

Three partitions, not one. Kafka guarantees ordering **within** a partition and nothing across them, so a single partition hides every ordering question you will meet later. Three is enough to see keys distribute unevenly, which they will.

### The subject name is a convention, not a rule

```python
def subject_for(topic: str) -> str:
    """TopicNameStrategy, the Confluent default.

    One subject per topic means one schema per topic, which is what makes a
    topic a typed stream rather than a byte pipe.
    """
    return f"{topic}-value"
```

`TopicNameStrategy` is one of three Confluent ships. The alternatives key the subject on the record type instead, which lets several event types share a topic — a real choice with real consequences for compatibility checking. The default is right here and worth knowing is a default.

### Registering over plain HTTP

```python
        request = urllib.request.Request(
            f"{cfg.schema_registry}/subjects/{subject}/versions",
            data=body,
            method="POST",
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        )
```

A client library would do this in one line. Doing it with `urllib` costs three and keeps the registry legible as what it is: a small REST service in front of a Kafka topic. The schema is posted as a *JSON string inside a JSON document*, which looks like a mistake and is not.

Posting the same schema twice returns the existing id rather than creating a version, so the bootstrap stays re-runnable.


## Step 5 — two gates, not one

```python
    catalog = CatalogConfig.from_env()
    streams = StreamConfig.from_env()
    if catalog is None and streams is None:
        log.error("nothing configured; set ICEBERG_WAREHOUSE or KAFKA_BOOTSTRAP_SERVERS")
        return 1
```

Kafka and the warehouse are configured independently. A batch-only deployment needs no broker; a stream-only one needs no catalog. Folding them into one flag would force every environment to run both.

Kafka is set up first, deliberately: it needs no Spark, so a Kafka-only bootstrap never starts a JVM.

*Verified* that the batch path is unaffected — `make warehouse` with no Kafka configured still creates the three Iceberg tables and never contacts a broker.


## Step 6 — check it

```bash
make test            # 50, no Docker
make up              # kafka and schema-registry now in the stack
make bootstrap       # creates topics and registers subjects
```

Then look at what exists:

```bash
# topics, from inside the network
docker compose -f docker/compose.yaml exec kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe

# subjects, from the host
curl -s localhost:8085/subjects | python3 -m json.tool
curl -s localhost:8085/subjects/ops.shipments-value/versions/1 | python3 -m json.tool
```

Expect `ops.shipments` and `ops.deliveries` with three partitions each, and two subjects whose schemas match the `.avsc` files.

Prove the two-listener story to yourself, which is worth more than reading about it:

```bash
# from the host, the HOST listener
docker run --rm --network host edenhill/kcat:1.7.1 -b localhost:29092 -L | head

# from inside, the INTERNAL listener
docker compose -f docker/compose.yaml exec kafka \
  /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka:9092 | head -2
```

Then break it: change `HOST://localhost:29092` to `HOST://kafka:29092`, `make down && make up`, and watch the host-side client connect and then hang.

Add the dependency with `uv add "confluent-kafka>=2.15,<3"` rather than editing the file by hand, so `uv.lock` is updated in the same step.


## Definition of done

- [ ] `make up` brings up Kafka and Schema Registry, both healthy
- [ ] `make bootstrap` creates two topics and registers two subjects, twice in a row
- [ ] `curl localhost:8085/subjects` lists both
- [ ] A client on the host can reach the broker on 29092
- [ ] `make warehouse` still works with no Kafka configured
- [ ] Changing an `.avsc` without changing the `StructType` fails a test


## Files in this PR

| File                                                  |                                                               |
| ----------------------------------------------------- | ------------------------------------------------------------- |
| `src/pipeline/streams.py`                             | new — topics, subjects, registration, and the config gate     |
| `src/pipeline/avro/shipment.avsc`, `delivery.avsc`    | new — the wire contract                                       |
| `tests/unit/test_streams.py`                          | new — ten tests, including Avro-versus-Spark drift            |
| `src/pipeline/bootstrap.py`                           | sets up Kafka too, gated separately from the catalog          |
| `docker/compose.yaml`                                 | Kafka in KRaft mode, Schema Registry on host port 8085        |
| `pyproject.toml`, `uv.lock`                           | `confluent-kafka`, and a scoped mypy override for it          |

Next: **PR 11** — a producer with controllable timing, which matters far more than throughput for the watermark experiments ahead.
