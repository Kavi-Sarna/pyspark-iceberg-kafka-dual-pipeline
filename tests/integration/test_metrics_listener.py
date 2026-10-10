"""The listener, against a real query and a real UDP socket.

The unit tests cover turning a progress event into numbers. What they cannot
cover is whether Spark ever calls the listener, whether the progress JSON has
the keys we read, and whether a datagram actually leaves the process. Those
are three separate ways for metrics to be silently absent in production, and
all three are only visible with a running query.

A rate source rather than Kafka: the question here is the listener, and a
rate source needs no broker to produce rows on a timer.
"""

from __future__ import annotations

import socket
from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

from pipeline.metrics import MetricsListener, Statsd, StatsdConfig


@pytest.fixture
def collector() -> Iterator[socket.socket]:
    """A real UDP socket on a free port, standing in for statsd-exporter."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 0))
    sock.settimeout(60)
    yield sock
    sock.close()


def drain(sock: socket.socket, deadline: int = 200) -> dict[str, float]:
    """Read datagrams until the socket goes quiet, parsed as StatsD gauges."""
    out: dict[str, float] = {}
    sock.settimeout(1)
    for _ in range(deadline):
        try:
            line = sock.recv(4096).decode()
        except TimeoutError:
            break
        name, _, rest = line.partition(":")
        value, _, kind = rest.partition("|")
        assert kind == "g", f"expected a gauge, got {line!r}"
        out[name] = float(value)
    return out


def test_a_running_query_emits_metrics(iceberg: SparkSession, collector: socket.socket) -> None:
    port = collector.getsockname()[1]
    listener = MetricsListener(
        Statsd(StatsdConfig(enabled=True, host="127.0.0.1", port=port, prefix="test"))
    )
    iceberg.streams.addListener(listener)
    try:
        query = (
            iceberg.readStream.format("rate")
            .option("rowsPerSecond", "10")
            .load()
            .writeStream.format("noop")
            .outputMode("append")
            .start()
        )
        try:
            # Progress is reported per trigger, so wait for a few of them.
            while query.lastProgress is None or query.lastProgress["batchId"] < 2:
                query.awaitTermination(1)
        finally:
            query.stop()
    finally:
        iceberg.streams.removeListener(listener)

    sent = drain(collector)

    # The listener was called at all, and the name carries the prefix.
    assert sent, "no datagrams arrived: Spark never called the listener"
    assert all(name.startswith("test.") for name in sent)

    # The keys the unit tests assume are really in Spark's progress JSON.
    assert "test.duration.triggerExecution" in sent
    assert "test.num_input_rows" in sent
    assert "test.processed_rows_per_second" in sent

    # A trigger takes a positive amount of time, and a rate source at ten rows
    # a second produces rows. If the parsing were wrong these would be zero.
    assert sent["test.duration.triggerExecution"] > 0
    assert sent["test.num_input_rows"] > 0


def test_a_query_with_no_state_reports_no_state_metrics(
    iceberg: SparkSession, collector: socket.socket
) -> None:
    """stateOperators is empty for a stateless query, and an empty list must
    produce no series rather than a zero. A flat zero would read as a join
    holding nothing, which is a different claim from having no join."""
    port = collector.getsockname()[1]
    listener = MetricsListener(
        Statsd(StatsdConfig(enabled=True, host="127.0.0.1", port=port, prefix="test"))
    )
    iceberg.streams.addListener(listener)
    try:
        query = (
            iceberg.readStream.format("rate")
            .option("rowsPerSecond", "10")
            .load()
            .writeStream.format("noop")
            .outputMode("append")
            .start()
        )
        try:
            while query.lastProgress is None or query.lastProgress["batchId"] < 1:
                query.awaitTermination(1)
        finally:
            query.stop()
    finally:
        iceberg.streams.removeListener(listener)

    sent = drain(collector)

    assert sent
    assert not [name for name in sent if ".state." in name]
    # The rate source reports plain offsets, not a topic/partition map, so
    # there is nothing to compute lag from either.
    assert not [name for name in sent if ".lag." in name]
