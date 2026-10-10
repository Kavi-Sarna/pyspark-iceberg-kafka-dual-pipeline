"""Kafka topics and Schema Registry subjects.

Separate from the Iceberg bootstrap because the two can be configured
independently: a batch-only deployment needs no Kafka, and a stream-only one
needs no warehouse.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import Any

log = logging.getLogger("pipeline.streams")

SHIPMENTS = "ops.shipments"
DELIVERIES = "ops.deliveries"


@dataclass(frozen=True)
class TopicSpec:
    partitions: int
    schema: str  # the .avsc basename


# Three partitions so orderin-per-key is visible and skew is possible; one
# replica because a single broker cannot do better. Both are wrong for
# production and right for a laptop.
TOPICS: Mapping[str, TopicSpec] = {
    SHIPMENTS: TopicSpec(partitions=3, schema="shipment"),
    DELIVERIES: TopicSpec(partitions=3, schema="delivery"),
}
REPLICATION = 1


@dataclass(frozen=True)
class StreamConfig:
    bootstrap_servers: str
    schema_registry: str


def subject_for(topic: str) -> str:
    """TopicNameStrategy, the Confluent default.

    One subject per topic means one schema per topic, which is what makes a
    topic a typed stream rather than a byte pipe.
    """
    return f"{topic}-value"


def load_schema(name: str) -> dict[str, Any]:
    path = Path(str(resources.files("pipeline").joinpath(f"avro/{name}.avsc")))
    parsed: dict[str, Any] = json.loads(path.read_text())
    return parsed


def create_topics(cfg: StreamConfig) -> None:
    # Imported here so this module loads without librdkafka: the pure
    # helpers above are used by code that never talks to a broker.
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": cfg.bootstrap_servers})
    existing = set(admin.list_topics(timeout=20).topics)
    wanted = [
        NewTopic(name, num_partitions=spec.partitions, replication_factor=REPLICATION)
        for name, spec in TOPICS.items()
        if name not in existing
    ]
    for name in TOPICS:
        if name in existing:
            log.info("ready   topic %s", name)
    for name, future in (admin.create_topics(wanted) if wanted else {}).items():
        future.result()
        log.info("created topic %s", name)


def register_subjects(cfg: StreamConfig) -> None:
    """Register each schema under its subject.

    Plain HTTP rather than a client library: the registry is a small REST API,
    and seeing the request makes the wire format easier to reason about later.
    """
    for topic, spec in TOPICS.items():
        subject = subject_for(topic)
        body = json.dumps(
            {"schemaType": "AVRO", "schema": json.dumps(load_schema(spec.schema))}
        ).encode()
        request = urllib.request.Request(
            f"{cfg.schema_registry}/subjects/{subject}/versions",
            data=body,
            method="POST",
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        )
        with urllib.request.urlopen(request, timeout=20) as response:
            schema_id = json.load(response)["id"]
        # Idempotent: posting an identical schema returns the existing id.
        log.info("ready   subject %s -> id %s", subject, schema_id)


def schema_id(cfg: StreamConfig, subject: str) -> int:
    """The registry's id for a subject's latest version.

    Producers need it because the Confluent wire format puts it in every
    message, so a consumer can fetch the exact schema a record was written
    with rather than assuming the current one.
    """
    url = f"{cfg.schema_registry}/subjects/{subject}/versions/latest"
    with urllib.request.urlopen(url, timeout=20) as response:
        found: int = json.load(response)["id"]
    return found


def setup(cfg: StreamConfig) -> None:
    create_topics(cfg)
    register_subjects(cfg)
