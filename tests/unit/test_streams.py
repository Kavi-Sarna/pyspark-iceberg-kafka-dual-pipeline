"""Topics, subjects and the Avro schemas, without a broker.

The schemas are a contract with anyone who reads these topics, so the point of
most of this is to make a silent change loud.
"""

from __future__ import annotations

import pytest

from pipeline.core.schemas import DELIVERY, SHIPMENT
from pipeline.streams import (
    DELIVERIES,
    REPLICATION,
    SHIPMENTS,
    TOPICS,
    StreamConfig,
    load_schema,
    subject_for,
)

SPARK_TO_AVRO = {"string": "string", "timestamp": "long"}


def avro_fields(name: str) -> list[tuple[str, str]]:
    """Field names and types, with logical types resolved to their base."""
    out = []
    for f in load_schema(name)["fields"]:
        t = f["type"]
        out.append((f["name"], t["type"] if isinstance(t, dict) else t))
    return out


@pytest.mark.parametrize(("avsc", "struct"), [("shipment", SHIPMENT), ("delivery", DELIVERY)])
def test_avro_matches_the_spark_schema(avsc: str, struct: object) -> None:
    """Two declarations of one event. They must not drift apart.

    Avro is the contract on the wire and StructType is the contract in the
    job; nothing but this test stops someone adding a field to one of them.
    """
    expected = [
        (f.name, SPARK_TO_AVRO[f.dataType.simpleString()])
        for f in struct.fields  # type: ignore[attr-defined]
    ]
    assert avro_fields(avsc) == expected


@pytest.mark.parametrize("avsc", ["shipment", "delivery"])
def test_event_time_is_a_logical_timestamp(avsc: str) -> None:
    """A bare long is an integer to every consumer. The logical type is what
    makes it a time, and it is what Spark reads to produce a timestamp."""
    schema = load_schema(avsc)
    times = [f for f in schema["fields"] if f["name"].endswith("_time")]
    assert times, "every event needs an event-time field"
    for f in times:
        assert f["type"]["logicalType"] == "timestamp-millis"


@pytest.mark.parametrize("avsc", ["shipment", "delivery"])
def test_no_field_is_nullable(avsc: str) -> None:
    """A union with null is how Avro spells optional. These events have none,
    matching the non-null StructTypes they mirror."""
    for f in load_schema(avsc)["fields"]:
        assert not isinstance(f["type"], list), f"{f['name']} is a union"


def test_subject_follows_topic_name_strategy() -> None:
    assert subject_for("ops.shipments") == "ops.shipments-value"


def test_both_topics_are_declared() -> None:
    assert set(TOPICS) == {SHIPMENTS, DELIVERIES}
    assert all(spec.partitions > 1 for spec in TOPICS.values())
    assert REPLICATION == 1


def test_from_env_returns_none_when_kafka_is_unconfigured() -> None:
    """A batch-only deployment must not need Kafka settings."""
    assert StreamConfig.from_env({}) is None
    assert StreamConfig.from_env({"SCHEMA_REGISTRY_URL": "http://x:8081"}) is None


def test_from_env_defaults_the_registry() -> None:
    cfg = StreamConfig.from_env({"KAFKA_BOOTSTRAP_SERVERS": "kafka:9092"})
    assert cfg is not None
    assert cfg.schema_registry == "http://schema-registry:8081"
