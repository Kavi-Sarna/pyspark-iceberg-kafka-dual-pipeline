"""The producer, without a broker.

Encoding and scenario shape are both testable offline, and both are things a
later failure would be hard to attribute: a wrong byte looks like a Spark
problem, and a wrong event time looks like a watermark problem.
"""

from __future__ import annotations

import io
import struct
from datetime import UTC, datetime
from typing import Any, cast

import fastavro
import pytest

from pipeline.producer import SCENARIOS, at, encode, parse_args, shipment
from pipeline.streams import DELIVERIES, SHIPMENTS, load_schema


def test_wire_format_is_magic_byte_then_id_then_body() -> None:
    """Five bytes of framing, then Avro. Nothing else goes in front."""
    raw = encode(
        {"shipment_id": "shp-1", "carrier": "c", "dispatch_time": at(0)},
        load_schema("shipment"),
        registry_id=7,
    )
    assert raw[0] == 0
    assert struct.unpack(">I", raw[1:5])[0] == 7
    assert len(raw) > 5


def test_body_decodes_with_the_schema_alone() -> None:
    """What the consumer will do: drop five bytes, read Avro.

    If this passes and Spark still cannot read the topic, the fault is in the
    stripping, not in the encoding.
    """
    record = {"shipment_id": "shp-1", "carrier": "carrier-a", "dispatch_time": at(0)}
    raw = encode(record, load_schema("shipment"), registry_id=1)

    schema = fastavro.parse_schema(load_schema("shipment"))
    # schemaless_reader is typed as returning a union; a record schema can
    # only yield a dict.
    back = cast(dict[str, Any], fastavro.schemaless_reader(io.BytesIO(raw[5:]), schema))

    assert back == record
    assert isinstance(back["dispatch_time"], datetime)


def test_the_schema_id_is_big_endian() -> None:
    """Confluent specifies network byte order. Little-endian would still be
    four bytes, decode to a plausible id, and fetch the wrong schema."""
    raw = encode(
        {"shipment_id": "s", "carrier": "c", "dispatch_time": at(0)},
        load_schema("shipment"),
        registry_id=258,  # 0x0102: byte order is visible
    )
    assert raw[1:5] == b"\x00\x00\x01\x02"


def test_event_time_does_not_depend_on_when_you_run_it() -> None:
    """A scenario must mean the same thing today and tomorrow, or no watermark
    assertion can be written against it."""
    assert at(0) == datetime(2026, 9, 13, 10, 0, tzinfo=UTC)
    assert at(5) == datetime(2026, 9, 13, 10, 5, tzinfo=UTC)
    assert shipment("shp-1", 0).value["dispatch_time"] == at(0)


def test_every_record_is_keyed_by_shipment_id() -> None:
    """Same key, same partition, so a shipment and its deliveries are ordered
    with respect to each other rather than racing."""
    for name, steps in SCENARIOS.items():
        for step in steps:
            expected = step.value.get("shipment_id")
            assert step.key == expected, f"{name}: {step.topic} keyed on {step.key}"


def test_scenarios_target_real_topics() -> None:
    for steps in SCENARIOS.values():
        assert all(s.topic in (SHIPMENTS, DELIVERIES) for s in steps)


def test_unmatched_sends_no_delivery() -> None:
    """The whole point of it: nothing can match, so nothing may be emitted
    until the watermark says so."""
    assert [s.topic for s in SCENARIOS["unmatched"]] == [SHIPMENTS]


def test_watermark_scenario_pauses_then_jumps_event_time() -> None:
    """A pause, then an event far enough ahead to release what is pending."""
    first, second = SCENARIOS["watermark"]
    assert first.wait == 0.0
    assert second.wait > 0
    assert second.value["dispatch_time"] > first.value["dispatch_time"]


def test_duplicate_has_two_deliveries_for_one_shipment() -> None:
    steps = SCENARIOS["duplicate"]
    assert sum(s.topic == DELIVERIES for s in steps) == 2
    assert len({s.key for s in steps}) == 1


def test_scenario_defaults_to_matched() -> None:
    assert parse_args([]).scenario == "matched"
    assert parse_args(["--scenario", "unmatched"]).scenario == "unmatched"


def test_unknown_scenario_is_rejected() -> None:
    with pytest.raises(SystemExit):
        parse_args(["--scenario", "chaos"])


def test_speed_divides_the_pauses() -> None:
    assert parse_args(["--speed", "10"]).speed == 10.0
