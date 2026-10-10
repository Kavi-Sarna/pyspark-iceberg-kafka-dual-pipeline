"""Metric extraction, against a progress event recorded from a real query.

The listener itself is three lines of plumbing. Everything that can be wrong
is in turning a progress event into numbers, and that is a dict in and a dict
out, so it needs neither Spark nor a socket.
"""

from __future__ import annotations

import json
import math
import socket
from typing import Any

import pytest

from pipeline.metrics import (
    MetricsListener,
    Statsd,
    StatsdConfig,
    kafka_lag,
    metrics_from,
)

SOURCE: dict[str, Any] = {
    "description": "KafkaV2[Subscribe[ops.shipments]]",
    "startOffset": {"ops.shipments": {"0": 10, "1": 0}},
    "endOffset": {"ops.shipments": {"0": 20, "1": 5}},
    "latestOffset": {"ops.shipments": {"0": 25, "1": 5}},
    "numInputRows": 15,
}

# Trimmed from an actual onQueryProgress event; the shapes are Spark's.
PROGRESS: dict[str, Any] = {
    "batchId": 7,
    "numInputRows": 20,
    "inputRowsPerSecond": 11.383039271485487,
    "processedRowsPerSecond": 50.63291139240506,
    "durationMs": {
        "addBatch": 218,
        "commitOffsets": 75,
        "getBatch": 0,
        "latestOffset": 0,
        "queryPlanning": 11,
        "triggerExecution": 395,
        "walCommit": 88,
    },
    "stateOperators": [
        {
            "operatorName": "stateStoreSave",
            "numRowsTotal": 3,
            "numRowsUpdated": 1,
            "numRowsRemoved": 2,
            "memoryUsedBytes": 1048,
            "numRowsDroppedByWatermark": 4,
        }
    ],
    "sources": [SOURCE],
}


def test_every_duration_phase_is_reported() -> None:
    """triggerExecution is the one to alert on; the rest say where it went."""
    out = metrics_from(PROGRESS)
    assert out["duration.triggerExecution"] == 395.0
    assert out["duration.addBatch"] == 218.0
    assert out["duration.walCommit"] == 88.0


def test_throughput_names_are_snake_case() -> None:
    out = metrics_from(PROGRESS)
    assert out["num_input_rows"] == 20.0
    assert out["input_rows_per_second"] == pytest.approx(11.383, rel=1e-3)
    assert out["processed_rows_per_second"] == pytest.approx(50.633, rel=1e-3)


def test_state_store_size_is_reported() -> None:
    """The number that answers why a streaming job uses the memory it does."""
    out = metrics_from(PROGRESS)
    assert out["state.0.rows"] == 3.0
    assert out["state.0.memory_bytes"] == 1048.0
    assert out["state.0.rows_removed"] == 2.0


def test_rows_dropped_by_watermark_is_reported() -> None:
    """Silent data loss otherwise. Spark drops late rows without complaint."""
    assert metrics_from(PROGRESS)["state.0.rows_dropped_by_watermark"] == 4.0


def test_lag_is_latest_minus_end_per_partition() -> None:
    """Spark commits no consumer-group offsets, so kafka-consumer-groups.sh
    shows nothing. Query progress is the only place lag exists."""
    lag = kafka_lag(SOURCE)
    assert lag == {"ops.shipments.0": 5, "ops.shipments.1": 0}


def test_lag_reaches_the_metric_names() -> None:
    out = metrics_from(PROGRESS)
    assert out["lag.ops.shipments.0"] == 5.0
    assert out["lag.ops.shipments.1"] == 0.0


def test_a_source_without_offsets_contributes_no_lag() -> None:
    """The rate and file sources report plain integers, not offset maps."""
    assert kafka_lag({"startOffset": None, "endOffset": 0, "latestOffset": 0}) == {}
    assert kafka_lag({}) == {}


def test_an_empty_batch_still_reports_durations() -> None:
    """A trigger with no data is exactly when you want to see the duration."""
    out = metrics_from({"durationMs": {"triggerExecution": 12}, "numInputRows": 0})
    assert out["duration.triggerExecution"] == 12.0
    assert out["num_input_rows"] == 0.0


def test_statsd_line_is_the_gauge_format(monkeypatch: pytest.MonkeyPatch) -> None:
    """name:value|g, one metric per datagram, prefixed."""
    sent: list[str] = []
    statsd = Statsd(StatsdConfig(enabled=True, host="h", port=1, prefix="pipeline.stream"))
    monkeypatch.setattr(statsd, "send", sent.append)

    statsd.gauge("state.0.rows", 3.0)

    assert sent == ["pipeline.stream.state.0.rows:3|g"]


def test_large_values_keep_every_digit(monkeypatch: pytest.MonkeyPatch) -> None:
    """The default %g formatting gives six significant digits and switches to
    exponential past a million, so a megabyte of state goes out as 1.04858e+06
    and comes back 1048580. A million is where these numbers start to matter."""
    sent: list[str] = []
    statsd = Statsd(StatsdConfig(enabled=True, host="127.0.0.1", port=1, prefix="p"))
    monkeypatch.setattr(statsd, "send", sent.append)

    statsd.gauge("state.0.memory_bytes", 1048576.0)
    statsd.gauge("lag.ops.shipments.0", 123456789.0)

    assert sent == ["p.state.0.memory_bytes:1048576|g", "p.lag.ops.shipments.0:123456789|g"]


def test_fractional_values_survive(monkeypatch: pytest.MonkeyPatch) -> None:
    sent: list[str] = []
    statsd = Statsd(StatsdConfig(enabled=True, host="127.0.0.1", port=1, prefix="p"))
    monkeypatch.setattr(statsd, "send", sent.append)

    statsd.gauge("input_rows_per_second", 11.383039271485487)

    assert sent == ["p.input_rows_per_second:11.383|g"]


def test_a_non_finite_value_is_not_sent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Spark reports inputRowsPerSecond as NaN on a first trigger and
    json.loads accepts it. "nan" is not something statsd-exporter can parse."""
    sent: list[str] = []
    statsd = Statsd(StatsdConfig(enabled=True, host="127.0.0.1", port=1, prefix="p"))
    monkeypatch.setattr(statsd, "send", sent.append)

    statsd.gauge("input_rows_per_second", float("nan"))
    statsd.gauge("input_rows_per_second", float("inf"))

    assert sent == []


def test_json_nan_reaches_the_client_as_nan() -> None:
    """The reason the guard above is needed rather than theoretical."""
    progress = json.loads('{"numInputRows": 0, "inputRowsPerSecond": NaN}')
    assert math.isnan(metrics_from(progress)["input_rows_per_second"])


def test_the_collector_is_resolved_once_not_per_metric() -> None:
    """setblocking(False) covers the send syscall, not the name lookup
    sendto() would do on every call. That lookup is synchronous whatever the
    socket's mode, and stalling the progress thread is the one thing a
    metrics client must not do."""
    statsd = Statsd(StatsdConfig(enabled=True, host="localhost", port=9125, prefix="p"))
    assert statsd._address == ("127.0.0.1", 9125)


def test_an_unresolvable_collector_disables_sending_rather_than_raising() -> None:
    statsd = Statsd(StatsdConfig(enabled=True, host="statsd.invalid.", port=9125, prefix="p"))
    assert statsd._address is None
    statsd.gauge("x", 1.0)  # must not raise


def test_a_dead_collector_does_not_raise() -> None:
    """Fire and forget. A metrics sink must never be able to stall a query."""
    statsd = Statsd(StatsdConfig(enabled=True, host="127.0.0.1", port=1, prefix="p"))
    statsd.gauge("x", 1.0)  # nothing is listening on port 1


def test_the_socket_is_non_blocking() -> None:
    """A blocking send to an unreachable host would stall the trigger."""
    statsd = Statsd(StatsdConfig(enabled=True, host="h", port=1, prefix="p"))
    assert statsd._socket.gettimeout() == 0.0
    assert statsd._socket.type == socket.SOCK_DGRAM


def test_lag_is_never_negative() -> None:
    """An unclean leader election can truncate a log, leaving the head behind
    where we already read. Negative records-behind means nothing."""
    lag = kafka_lag(
        {
            "endOffset": {"ops.shipments": {"0": 100}},
            "latestOffset": {"ops.shipments": {"0": 40}},
        }
    )
    assert lag == {"ops.shipments.0": 0}


def test_a_partition_with_no_head_is_omitted_not_zeroed() -> None:
    """Zero would claim the partition is caught up. We do not know that."""
    lag = kafka_lag(
        {
            "endOffset": {"ops.shipments": {"0": 10, "1": 7}},
            "latestOffset": {"ops.shipments": {"0": 12}},
        }
    )
    assert lag == {"ops.shipments.0": 2}


def test_a_broken_progress_event_does_not_escape_the_listener() -> None:
    """Spark's listener bus does catch this, but reports it as a JVM error on
    every trigger. A metrics path should fail quietly and on purpose."""

    class Event:
        progress = type("P", (), {"json": "not json at all"})()

    listener = MetricsListener(
        Statsd(StatsdConfig(enabled=True, host="127.0.0.1", port=1, prefix="p"))
    )
    listener.onQueryProgress(Event())  # must not raise
