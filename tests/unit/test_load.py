"""The generated stream, checked without a broker.

records() is pure and seeded, so everything that decides whether a demo is
worth watching - how many rows never match, how many arrive too late, whether
event time advances at all - is a list comprehension away from an assertion.
"""

from __future__ import annotations

import time
from datetime import timedelta
from typing import Any

import pytest

from pipeline.load import BASE, LoadSpec, records
from pipeline.streams import DELIVERIES, SHIPMENTS

MAX_LAG = timedelta(minutes=30)


def generated(**overrides: Any) -> list[tuple[str, str, dict[str, Any]]]:
    """The body of the stream. The closing pair is off here because it has its
    own tests below, and leaving it on would put a +1 in every count."""
    overrides.setdefault("flush", False)
    spec = LoadSpec(shipments=1000, **overrides)
    return list(records(spec, MAX_LAG))


def test_every_shipment_is_emitted_once() -> None:
    rows = generated()
    shipments = [r for r in rows if r[0] == SHIPMENTS]
    assert len(shipments) == 1000
    assert len({r[1] for r in shipments}) == 1000


def test_the_match_rate_is_what_it_says() -> None:
    """The unmatched remainder is the left join's whole reason to exist, so
    the number of them is a setting rather than an accident."""
    rows = generated(match_rate=0.9)
    deliveries = [r for r in rows if r[0] == DELIVERIES]

    assert 0.86 < len(deliveries) / 1000 < 0.94


def test_nothing_matches_when_the_match_rate_is_zero() -> None:
    assert [r for r in generated(match_rate=0.0) if r[0] == DELIVERIES] == []


def test_a_delivery_shares_its_shipment_key() -> None:
    """Same key means same partition, so their relative order is a fact
    rather than a race."""
    rows = generated()
    by_key: dict[str, set[str]] = {}
    for topic, key, _ in rows:
        by_key.setdefault(key, set()).add(topic)
    paired = [k for k, topics in by_key.items() if len(topics) == 2]
    assert len(paired) > 800
    for key in paired:
        assert key.startswith("shp-")


def test_event_time_advances_across_the_run() -> None:
    """Without this the watermark never moves, nothing is ever released from
    state, and the demo is a flat line."""
    rows = [r for r in generated(window=timedelta(hours=2)) if r[0] == SHIPMENTS]
    times = [r[2]["dispatch_time"] for r in rows]

    assert times == sorted(times)
    assert times[0] == BASE
    assert times[-1] - times[0] >= timedelta(minutes=119)


def test_on_time_deliveries_land_inside_the_join_window() -> None:
    """Outside it they would not join, and the demo would show a match rate
    that has nothing to do with the configured one."""
    rows = generated(late_rate=0.0)
    dispatch = {r[1]: r[2]["dispatch_time"] for r in rows if r[0] == SHIPMENTS}

    for topic, key, value in rows:
        if topic != DELIVERIES:
            continue
        lag = value["delivered_time"] - dispatch[key]
        assert timedelta(0) <= lag <= MAX_LAG


def test_some_deliveries_are_deliberately_too_late() -> None:
    """A dashboard panel that can only ever read zero teaches nothing about
    the panel. These are the rows that make rows_dropped_by_watermark move."""
    rows = generated(late_rate=0.2, window=timedelta(hours=2))
    dispatch = {r[1]: r[2]["dispatch_time"] for r in rows if r[0] == SHIPMENTS}
    late = [
        key
        for topic, key, value in rows
        if topic == DELIVERIES and value["delivered_time"] < dispatch[key]
    ]

    assert len(late) > 100


def test_the_same_seed_generates_the_same_stream() -> None:
    assert generated(seed=1) == generated(seed=1)
    assert generated(seed=1) != generated(seed=2)


def test_the_reported_total_is_close_to_what_is_generated() -> None:
    """main() prints it before sending, so a mistyped flag is visible before a
    hundred thousand records are on a topic. It is an estimate: whether a
    given shipment gets a delivery is a coin weighted by match_rate, so the
    exact count is only known once the stream has been generated."""
    spec = LoadSpec(shipments=2000, match_rate=0.9)
    actual = len(list(records(spec, MAX_LAG)))

    assert spec.deliveries == 1800
    assert abs(spec.total - actual) < 0.03 * actual


@pytest.mark.parametrize("count", [0, 1, 7])
def test_small_counts_do_not_divide_by_zero(count: int) -> None:
    rows = list(records(LoadSpec(shipments=count, flush=False), MAX_LAG))
    assert len([r for r in rows if r[0] == SHIPMENTS]) == count


# --- the closing pair -------------------------------------------------------

LATENESS = timedelta(minutes=10)


def test_the_closing_pair_releases_every_unmatched_shipment() -> None:
    """Without it the watermark freezes where the data stopped.

    A left join emits an unmatched row when the watermark passes
    dispatch + max_lag, and the watermark only moves when records arrive. The
    stream is bounded, so the last shipments would sit in state forever - and
    a demo that says "10,058 released" would be wrong by thousands."""
    rows = list(records(LoadSpec(shipments=1000), MAX_LAG, LATENESS))
    shipments = {r[1]: r[2]["dispatch_time"] for r in rows if r[0] == SHIPMENTS}
    deliveries = {r[1]: r[2]["delivered_time"] for r in rows if r[0] == DELIVERIES}

    # The global watermark is the minimum across inputs, not the newest event.
    watermark = min(max(shipments.values()), max(deliveries.values())) - LATENESS
    threshold = watermark - MAX_LAG

    stuck = [k for k, t in shipments.items() if k not in deliveries and t > threshold]
    assert stuck == []


def test_without_the_closing_pair_the_tail_is_stranded() -> None:
    """The same arithmetic, with flush off: this is what it is protecting."""
    rows = list(records(LoadSpec(shipments=1000, flush=False), MAX_LAG, LATENESS))
    shipments = {r[1]: r[2]["dispatch_time"] for r in rows if r[0] == SHIPMENTS}
    deliveries = {r[1]: r[2]["delivered_time"] for r in rows if r[0] == DELIVERIES}

    watermark = min(max(shipments.values()), max(deliveries.values())) - LATENESS
    threshold = watermark - MAX_LAG

    stuck = [k for k, t in shipments.items() if k not in deliveries and t > threshold]
    assert stuck, "flush=False should strand the tail; otherwise flush proves nothing"


def test_the_closing_pair_lands_on_both_topics() -> None:
    """One topic would not move the watermark: it is the minimum of the two."""
    rows = list(records(LoadSpec(shipments=10), MAX_LAG, LATENESS))
    last_two = rows[-2:]

    assert {r[0] for r in last_two} == {SHIPMENTS, DELIVERIES}
    assert {r[1] for r in last_two} == {"shp-flush"}


def test_the_closing_pair_matches_itself() -> None:
    """Otherwise it would be one more unmatched row, in the number people
    are about to read off a dashboard."""
    rows = list(records(LoadSpec(shipments=10), MAX_LAG, LATENESS))
    pair = {r[0]: r[2] for r in rows[-2:]}
    dispatch = pair[SHIPMENTS]["dispatch_time"]
    delivered = pair[DELIVERIES]["delivered_time"]

    assert dispatch <= delivered <= dispatch + MAX_LAG


# --- sending ----------------------------------------------------------------


class FakeProducer:
    """Enough of confluent_kafka.Producer to exercise run()."""

    def __init__(self, conf: dict[str, Any]) -> None:
        self.conf = conf
        self.sent: list[tuple[str, bytes, bytes]] = []
        self.undelivered = 0
        self.refuse = 0

    def produce(self, topic: str, key: bytes, value: bytes) -> None:
        if self.refuse:
            self.refuse -= 1
            raise BufferError("queue full")
        self.sent.append((topic, key, value))

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        return self.undelivered


@pytest.fixture
def fake_kafka(monkeypatch: pytest.MonkeyPatch) -> FakeProducer:
    import confluent_kafka

    import pipeline.load as load_module

    producer = FakeProducer({})
    monkeypatch.setattr(confluent_kafka, "Producer", lambda conf: producer)
    monkeypatch.setattr(load_module, "schema_id", lambda cfg, subject: 1)
    monkeypatch.setattr(time, "sleep", lambda seconds: None)
    return producer


def test_every_generated_record_is_produced(fake_kafka: FakeProducer) -> None:
    from pipeline.load import run
    from pipeline.streams import StreamConfig

    spec = LoadSpec(shipments=50, rate=0)
    expected = len(list(records(spec, MAX_LAG, LATENESS)))

    sent = run(StreamConfig("broker:9092", "http://registry:8081"), spec, MAX_LAG, LATENESS)

    assert sent == len(fake_kafka.sent) == expected


def test_an_undelivered_record_is_an_error_not_a_count(fake_kafka: FakeProducer) -> None:
    """flush() returns what never left. A demo that ignores it reports a
    number the table cannot match."""
    from pipeline.load import run
    from pipeline.streams import StreamConfig

    fake_kafka.undelivered = 3

    with pytest.raises(RuntimeError, match="never reached the broker"):
        run(
            StreamConfig("broker:9092", "http://registry:8081"),
            LoadSpec(shipments=5),
            MAX_LAG,
            LATENESS,
        )


def test_a_full_local_queue_is_retried_rather_than_lost(fake_kafka: FakeProducer) -> None:
    """BufferError means the broker is behind, not that the record is bad."""
    from pipeline.load import run
    from pipeline.streams import StreamConfig

    fake_kafka.refuse = 3
    spec = LoadSpec(shipments=10, rate=0)
    expected = len(list(records(spec, MAX_LAG, LATENESS)))

    sent = run(StreamConfig("broker:9092", "http://registry:8081"), spec, MAX_LAG, LATENESS)

    assert sent == expected, "a refused record must be retried, not dropped"
