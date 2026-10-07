"""The Kafka half, against a real broker started by testcontainers.

The watermark tests cover what the join means; these cover whether the bytes
survive the trip. Different failures, so different tests: a semantics bug and
a serialization bug look nothing alike and should not be found by the same
assertion.

Skipped when no Docker daemon is reachable, so the suite still runs on a
laptop without one.
"""

from __future__ import annotations

import json
from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline.io.kafka import decode
from pipeline.producer import SCENARIOS, encode
from pipeline.streams import SHIPMENTS, load_schema, subject_for

docker = pytest.importorskip("docker", reason="testcontainers needs the docker package")
testcontainers = pytest.importorskip("testcontainers.community.kafka")


def _docker_is_up() -> bool:
    try:
        docker.from_env().ping()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _docker_is_up(), reason="no Docker daemon; these need a real broker"
)


@pytest.fixture(scope="module")
def broker() -> Iterator[str]:
    """A single-broker Kafka, torn down with the module."""
    from testcontainers.community.kafka import KafkaContainer

    with KafkaContainer() as container:
        yield container.get_bootstrap_server()


@pytest.fixture
def topic(broker: str) -> str:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": broker})
    name = SHIPMENTS
    if name not in admin.list_topics(timeout=20).topics:
        for future in admin.create_topics(
            [NewTopic(name, num_partitions=3, replication_factor=1)]
        ).values():
            future.result()
    return name


def produce(broker: str, topic: str, schema_id: int = 1) -> int:
    """Send the matched scenario with our own framing."""
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": broker})
    steps = [s for s in SCENARIOS["matched"] if s.topic == topic]
    for step in steps:
        producer.produce(
            topic=topic,
            key=step.key.encode(),
            value=encode(step.value, load_schema("shipment"), schema_id),
        )
    producer.flush(30)
    return len(steps)


def test_the_broker_starts_and_the_topic_exists(broker: str, topic: str) -> None:
    from confluent_kafka.admin import AdminClient

    meta = AdminClient({"bootstrap.servers": broker}).list_topics(timeout=20)
    assert topic in meta.topics
    assert len(meta.topics[topic].partitions) == 3


def test_a_record_round_trips_through_a_real_broker(
    iceberg: SparkSession, broker: str, topic: str
) -> None:
    """Producer bytes, through Kafka, decoded by the adapter.

    This is the test that would have caught the framing being wrong, which no
    amount of in-memory encoding checks can: the producer and the consumer
    agreeing with each other proves nothing if both are wrong the same way,
    but a real broker at least proves the bytes are not mangled in transit.
    """
    sent = produce(broker, topic)
    assert sent > 0

    raw = (
        iceberg.read.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )
    rows = decode(raw, load_schema("shipment")).collect()

    assert [r["shipment_id"] for r in rows] == ["shp-1"]
    assert rows[0]["carrier"] == "carrier-a"


def test_the_key_is_the_shipment_id(iceberg: SparkSession, broker: str, topic: str) -> None:
    """Keying is what makes a shipment and its deliveries share a partition."""
    produce(broker, topic)

    keys = (
        iceberg.read.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .select(F.col("key").cast("string").alias("k"))
        .collect()
    )

    assert {r["k"] for r in keys} == {"shp-1"}


def test_the_first_five_bytes_are_the_framing(
    iceberg: SparkSession, broker: str, topic: str
) -> None:
    """Read the raw value and look at it, rather than trusting the decoder."""
    produce(broker, topic, schema_id=258)

    rows = (
        iceberg.read.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .select("value")
        .collect()
    )
    value = rows[-1]["value"]
    assert value[0] == 0
    assert int.from_bytes(value[1:5], "big") == 258


def test_subject_naming_is_what_the_registry_will_be_asked_for() -> None:
    """No broker needed, but it belongs with the wire tests."""
    assert subject_for(SHIPMENTS) == f"{SHIPMENTS}-value"
    assert json.loads(json.dumps(load_schema("shipment")))["name"] == "Shipment"
