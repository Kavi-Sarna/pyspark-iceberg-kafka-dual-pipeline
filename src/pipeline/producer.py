"""A producer you can reason about.

Not a load generator. The streaming questions ahead are about *when* an event
arrives relative to another, so this trades throughput for two kinds of
control:

  event time   fixed offsets from a base, so a scenario means the same thing
               every run regardless of the wall clock
  arrival      an explicit pause before each send, so a watermark can be
               held still and then advanced on purpose
"""

from __future__ import annotations

import argparse
import io
import logging
import struct
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import fastavro

from pipeline.streams import DELIVERIES, SHIPMENTS, TOPICS, StreamConfig, schema_id, subject_for

log = logging.getLogger("pipeline.producer")

MAGIC = b"\x00"
BASE = datetime(2026, 9, 13, 10, 0, tzinfo=UTC)


def at(minutes: int) -> datetime:
    """Event time, as an offset from a fixed base rather than from now."""
    return BASE + timedelta(minutes=minutes)


@dataclass(frozen=True)
class Step:
    """One record, and how long to wait before sending it."""

    wait: float  # seconds of wall clock before this send
    topic: str
    key: str
    value: Mapping[str, Any]


def shipment(sid: str, minutes: int, carrier: str = "carrier-a") -> Step:
    return Step(
        0.0,
        SHIPMENTS,
        sid,
        {"shipment_id": sid, "carrier": carrier, "dispatch_time": at(minutes)},
    )


def delivery(did: str, sid: str, minutes: int) -> Step:
    return Step(
        0.0,
        DELIVERIES,
        sid,
        {"delivery_id": did, "shipment_id": sid, "delivered_time": at(minutes)},
    )


def after(seconds: float, step: Step) -> Step:
    """Pause before sending. This is the knob the watermark tests turn."""
    return Step(seconds, step.topic, step.key, step.value)


# Keyed by shipment_id throughout, so a shipment and its deliveries land on the
# same partition and their relative order is a fact rather than a race.
SCENARIOS: Mapping[str, Sequence[Step]] = {
    # One shipment, one delivery, two minutes apart in event time.
    "matched": [shipment("shp-1", 0), delivery("dlv-1", "shp-1", 2)],
    # A shipment with no delivery, ever. The join cannot emit it until the
    # watermark proves no match can still arrive.
    "unmatched": [shipment("shp-2", 5)],
    # A later shipment and nothing else: event time moves forward, which is
    # what releases the pending row above.
    "advance": [shipment("shp-9", 120)],
    # Two deliveries for one shipment: the row multiplication, on a stream.
    "duplicate": [
        shipment("shp-3", 10),
        delivery("dlv-2", "shp-3", 11),
        delivery("dlv-3", "shp-3", 12),
    ],
    # A delivery whose event time precedes its shipment's. Nonsense in the
    # domain, and worth having on hand to see what the join does with it.
    "out-of-order": [shipment("shp-4", 20), delivery("dlv-4", "shp-4", 15)],
    # The unmatched case and its release, in one run with a pause between.
    "watermark": [
        shipment("shp-5", 30),
        after(10.0, shipment("shp-8", 180)),
    ],
}


def encode(record: Mapping[str, Any], schema: Mapping[str, Any], registry_id: int) -> bytes:
    """Confluent wire format: magic byte, 4-byte big-endian id, Avro body.

    Written by hand because the consumer has to strip it by hand: Spark's
    from_avro takes a schema string and knows nothing about a registry.
    """
    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, fastavro.parse_schema(dict(schema)), dict(record))
    return MAGIC + struct.pack(">I", registry_id) + buffer.getvalue()


def run(cfg: StreamConfig, steps: Sequence[Step], speed: float = 1.0) -> int:
    from confluent_kafka import Producer

    from pipeline.streams import load_schema

    schemas = {
        topic: (load_schema(spec.schema), schema_id(cfg, subject_for(topic)))
        for topic, spec in TOPICS.items()
    }
    producer = Producer({"bootstrap.servers": cfg.bootstrap_servers})

    for step in steps:
        if step.wait:
            log.info("wait    %.1fs", step.wait / speed)
            time.sleep(step.wait / speed)
        schema, registry_id = schemas[step.topic]
        producer.produce(
            topic=step.topic,
            key=step.key.encode(),
            value=encode(step.value, schema, registry_id),
        )
        producer.poll(0)
        log.info("sent    %-14s %s", step.topic, dict(step.value))

    producer.flush(30)
    return len(steps)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="pipeline-producer")
    parser.add_argument("--scenario", choices=sorted(SCENARIOS), default="matched")
    parser.add_argument("--speed", type=float, default=1.0, help="divide every pause by this")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    args = parse_args(argv)

    cfg = StreamConfig.from_env()
    if cfg is None:
        log.error("no Kafka configured; set KAFKA_BOOTSTRAP_SERVERS")
        return 1

    sent = run(cfg, SCENARIOS[args.scenario], args.speed)
    log.info("done    %s, %d record(s)", args.scenario, sent)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
