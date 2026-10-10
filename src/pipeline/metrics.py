"""Streaming metrics, pushed to StatsD.

A StreamingQueryListener is the only place some of these numbers exist. Batch
duration, state-store size and Kafka lag are reported in query progress and
nowhere else — in particular there is no consumer group to inspect, because
Spark keeps its offsets in the checkpoint rather than committing them to
Kafka, so the usual lag tooling reports nothing at all.

The StatsD client is twenty lines of UDP rather than a dependency. The
protocol is one line per metric and writing it keeps visible what would
otherwise be a library call.
"""

from __future__ import annotations

import json
import logging
import math
import socket
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from pyspark.sql.streaming.listener import StreamingQueryListener

log = logging.getLogger("pipeline.metrics")


@dataclass(frozen=True)
class StatsdConfig:
    enabled: bool
    host: str
    port: int
    prefix: str


class Statsd:
    """Fire-and-forget UDP. A metrics sink must never be able to stall a job."""

    def __init__(self, cfg: StatsdConfig) -> None:
        self._cfg = cfg
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._socket.setblocking(False)
        self._address = self._resolve()

    def _resolve(self) -> tuple[str, int] | None:
        """Look the collector up once, at startup.

        setblocking(False) covers the send syscall and not the name lookup
        that sendto() performs on every call when given a hostname. That
        lookup is synchronous whatever the socket's mode, so a slow resolver
        would stall the progress thread once per metric per trigger - the
        exact stall this class exists to avoid.

        The address is therefore fixed for the life of the process, which is
        the right trade for a sidecar and wrong for a collector that changes
        address under us.
        """
        try:
            info = socket.getaddrinfo(
                self._cfg.host, self._cfg.port, socket.AF_INET, socket.SOCK_DGRAM
            )
        except OSError:
            log.warning("metrics host %r does not resolve; metrics are off", self._cfg.host)
            return None
        address = info[0][4]
        return (str(address[0]), int(address[1]))

    def gauge(self, name: str, value: float) -> None:
        # A non-finite value is not a reading. Spark reports inputRowsPerSecond
        # as NaN on a first trigger, json.loads accepts it, and "nan" is not a
        # number statsd-exporter can parse.
        if not math.isfinite(value):
            return
        self.send(f"{self._cfg.prefix}.{name}:{_number(value)}|g")

    def send(self, line: str) -> None:
        if self._address is None:
            return
        try:
            self._socket.sendto(line.encode(), self._address)
        except OSError:
            # UDP to a host that is not listening still raises on some
            # platforms. Losing a metric must not take the query with it.
            log.debug("dropped metric: %s", line, exc_info=True)


def _number(value: float) -> str:
    """Enough digits for the readings that matter.

    Python's %g gives six significant digits and switches to exponential
    notation past a million, so 1048576 bytes of state goes out as 1.04858e+06
    and comes back 1048580. A million is not an unusual state store or an
    unusual lag; it is the region where the number starts to matter.
    """
    return f"{value:.3f}".rstrip("0").rstrip(".")


def kafka_lag(source: Mapping[str, Any]) -> dict[str, int]:
    """Records available but not yet read, per topic partition.

    endOffset is where this batch finished; latestOffset is where the topic
    now ends. Their difference is the lag, and it is the only way to get it:
    Spark commits nothing to a consumer group.
    """
    end, latest = source.get("endOffset"), source.get("latestOffset")
    if not isinstance(end, dict) or not isinstance(latest, dict):
        return {}

    # Iterating latest, not end: a partition we have never read is lag we owe.
    # A partition in end but missing from latest is left out rather than
    # reported as zero, because we do not know where its head is and zero
    # would claim it is caught up.
    out: dict[str, int] = {}
    for topic, partitions in latest.items():
        if not isinstance(partitions, dict):
            continue
        done = end.get(topic, {}) if isinstance(end.get(topic), dict) else {}
        for partition, head in partitions.items():
            # Clamped: an unclean leader election can truncate a log, leaving
            # the head behind where we already read. Negative records-behind
            # is not a thing a dashboard can mean.
            behind = int(head) - int(done.get(partition, 0))
            out[f"{topic}.{partition}"] = max(behind, 0)
    return out


def metrics_from(progress: Mapping[str, Any]) -> dict[str, float]:
    """Everything worth graphing, from one progress event.

    Pure: a dict in, a dict out. The listener below only has to send these,
    which is why this is the part with tests.
    """
    out: dict[str, float] = {}

    durations = progress.get("durationMs") or {}
    for phase, value in durations.items():
        out[f"duration.{phase}"] = float(value)

    for key in ("numInputRows", "inputRowsPerSecond", "processedRowsPerSecond"):
        value = progress.get(key)
        if isinstance(value, int | float):
            out[_snake(key)] = float(value)

    for index, operator in enumerate(progress.get("stateOperators") or []):
        # The number that answers "why is this job using so much memory".
        out[f"state.{index}.rows"] = float(operator.get("numRowsTotal", 0))
        out[f"state.{index}.memory_bytes"] = float(operator.get("memoryUsedBytes", 0))
        out[f"state.{index}.rows_removed"] = float(operator.get("numRowsRemoved", 0))
        # Rows arriving after the watermark are dropped silently. A non-zero
        # value here is data loss you would otherwise never see.
        out[f"state.{index}.rows_dropped_by_watermark"] = float(
            operator.get("numRowsDroppedByWatermark", 0)
        )

    for source in progress.get("sources") or []:
        for partition, lag in kafka_lag(source).items():
            out[f"lag.{partition}"] = float(lag)

    return out


def _snake(name: str) -> str:
    return "".join(f"_{c.lower()}" if c.isupper() else c for c in name).lstrip("_")


class MetricsListener(StreamingQueryListener):
    """Sends on every trigger. Registered on the session, not the query."""

    def __init__(self, statsd: Statsd) -> None:
        self._statsd = statsd

    def onQueryStarted(self, event: Any) -> None:  # noqa: N802 - Spark's name
        log.info("query   started %s", event.id)

    def onQueryProgress(self, event: Any) -> None:  # noqa: N802 - Spark's name
        # One intentional boundary. Spark's listener bus does catch what a
        # listener throws, but it reports it as a JVM error on every trigger,
        # and the point of a metrics path is that it fails quietly.
        try:
            progress = json.loads(event.progress.json)
            for name, value in metrics_from(progress).items():
                self._statsd.gauge(name, value)
        except Exception:
            log.warning("could not report metrics for this trigger", exc_info=True)

    def onQueryTerminated(self, event: Any) -> None:  # noqa: N802 - Spark's name
        log.info("query   terminated %s", event.id)
