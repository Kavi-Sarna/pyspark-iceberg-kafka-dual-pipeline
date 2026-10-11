"""A load generator, which producer.py deliberately is not.

producer.py exists to reason about one event at a time: fixed event times,
explicit pauses, six scenarios you can hold in your head. This module is for
the opposite need - enough records that the Spark UI has stages worth looking
at and the Grafana panels have a shape rather than a dot.

Two things make the output worth watching rather than merely large:

  event time advances with the run, so the watermark moves, state turns over,
  and unmatched rows are released while you watch instead of at the end

  a deliberate minority of records misbehave, so rows_dropped_by_watermark and
  the null-padded left-join rows are non-zero on a dashboard that would
  otherwise show a flat, uninformative success

The record stream is a pure generator. Rate limiting and Kafka live in run().
"""

from __future__ import annotations

import argparse
import logging
import random
import time
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from pipeline.config import AppConfig, parse_duration
from pipeline.producer import encode
from pipeline.streams import DELIVERIES, SHIPMENTS, TOPICS, StreamConfig, schema_id, subject_for

log = logging.getLogger("pipeline.load")

BASE = datetime(2026, 9, 13, 10, 0, tzinfo=UTC)

CARRIERS = ("carrier-a", "carrier-b", "carrier-c", "carrier-d")


@dataclass(frozen=True)
class LoadSpec:
    """What to generate. Defaults produce a demo that lasts about a minute."""

    shipments: int = 100_000
    # Records per second, counting both topics. 0 means as fast as the broker
    # will take them, which is a throughput test rather than a demonstration.
    rate: float = 2_000.0
    # The fraction of shipments that ever get a delivery. The rest are the
    # left join's reason to exist: held in state, then emitted null-padded.
    match_rate: float = 0.9
    # Of those deliveries, the fraction sent with an event time far enough
    # behind the watermark to be dropped. Non-zero on purpose: a dashboard
    # that can only ever show zero teaches nothing about the panel.
    late_rate: float = 0.02
    # Event time spanned by the whole run, compressed into however long the
    # run actually takes. Wider means the watermark moves faster per record.
    window: timedelta = timedelta(hours=2)
    seed: int = 7
    # A matched pair sent far ahead in event time, on both topics, once the
    # real records are done. Without it the watermark freezes where the data
    # stopped and the last shipments never leave the state store - see
    # flush_at() for the arithmetic.
    flush: bool = True

    @property
    def deliveries(self) -> int:
        return int(self.shipments * self.match_rate)

    @property
    def total(self) -> int:
        """An estimate, for the progress line. Whether a shipment gets a
        delivery is a weighted coin, so the exact count is only known once the
        stream has been generated."""
        extra = 2 if self.flush else 0
        return self.shipments + self.deliveries + extra


def flush_at(spec: LoadSpec, lateness: timedelta, max_lag: timedelta) -> datetime:
    """The event time the closing pair needs, for every shipment to be released.

    A stream-stream left join emits an unmatched row once the watermark proves
    no partner can still arrive, which is when the watermark passes
    dispatch + max_lag. The watermark is itself the newest event time seen,
    less the allowed lateness - and it is the *minimum* across both inputs, so
    a pair is needed rather than one record.

    Setting the last shipment at BASE + window, that gives

        flush - lateness - max_lag  >=  BASE + window

    and the margin is there so the boundary is not a coin toss.
    """
    return BASE + spec.window + lateness + max_lag + timedelta(minutes=10)


def records(
    spec: LoadSpec, max_lag: timedelta, lateness: timedelta = timedelta(minutes=10)
) -> Iterator[tuple[str, str, dict[str, Any]]]:
    """(topic, key, value), in the order they should be sent.

    Pure and deterministic given the seed, so the shape of a run can be
    asserted without a broker anywhere near it.

    Keyed by shipment_id on both topics, so a shipment and its delivery share
    a partition and their relative order is a fact rather than a race.
    """
    rng = random.Random(spec.seed)
    step = spec.window / max(spec.shipments, 1)
    # Deliveries land inside the join's time bound, with room to spare: a
    # delivery exactly on the boundary is a coin toss, not a demonstration.
    reach = max_lag * 0.8

    for index in range(spec.shipments):
        shipment_id = f"shp-{index:07d}"
        dispatch = BASE + step * index
        yield (
            SHIPMENTS,
            shipment_id,
            {
                "shipment_id": shipment_id,
                "carrier": CARRIERS[index % len(CARRIERS)],
                "dispatch_time": dispatch,
            },
        )

        if rng.random() >= spec.match_rate:
            continue  # no delivery, ever: this one tests the left join

        if rng.random() < spec.late_rate:
            # Far enough behind that the watermark has already passed it.
            delivered = dispatch - spec.window
        else:
            delivered = dispatch + reach * rng.random()

        yield (
            DELIVERIES,
            shipment_id,
            {
                "delivery_id": f"dlv-{index:07d}",
                "shipment_id": shipment_id,
                "delivered_time": delivered,
            },
        )

    if not spec.flush:
        return

    # The closing pair. Matched, so it does not become an eleven-thousand-and
    # -first unmatched row, and on both topics because the watermark is the
    # minimum across them: advancing one leaves the join exactly where it was.
    moment = flush_at(spec, lateness, max_lag)
    yield (
        SHIPMENTS,
        "shp-flush",
        {"shipment_id": "shp-flush", "carrier": CARRIERS[0], "dispatch_time": moment},
    )
    yield (
        DELIVERIES,
        "shp-flush",
        {
            "delivery_id": "dlv-flush",
            "shipment_id": "shp-flush",
            "delivered_time": moment + timedelta(seconds=1),
        },
    )


def run(cfg: StreamConfig, spec: LoadSpec, max_lag: timedelta, lateness: timedelta) -> int:
    """Send the stream, pacing it so the dashboards have time to move."""
    from confluent_kafka import Producer

    from pipeline.streams import load_schema

    schemas = {
        topic: (load_schema(topic_spec.schema), schema_id(cfg, subject_for(topic)))
        for topic, topic_spec in TOPICS.items()
    }
    # linger.ms batches rather than sending one request per record; without it
    # the rate limit below is not what decides throughput.
    producer = Producer(
        {
            "bootstrap.servers": cfg.bootstrap_servers,
            "linger.ms": 20,
            "batch.size": 64 * 1024,
            "compression.type": "lz4",
            "queue.buffering.max.messages": 1_000_000,
        }
    )

    # Pace roughly once a second rather than every thousand records. At the
    # default rate those are the same thing; at RATE=10 a fixed count would
    # send a thousand records at once and then sleep for a minute and a half,
    # which is a sawtooth rather than a stream.
    every = max(1, min(1000, int(spec.rate))) if spec.rate > 0 else 1000

    started = time.monotonic()
    sent = 0
    for topic, key, value in records(spec, max_lag, lateness):
        schema, registry_id = schemas[topic]
        payload = encode(value, schema, registry_id)
        blocked = time.monotonic()
        while True:
            try:
                producer.produce(topic=topic, key=key.encode(), value=payload)
                break
            except BufferError:
                # The local queue is full, so the broker is the bottleneck
                # rather than the pacing below. Let it drain - but say so,
                # because a silent wait here looks like a frozen terminal.
                waited = time.monotonic() - blocked
                if waited > 30:
                    raise RuntimeError(
                        f"the broker has accepted nothing for {waited:.0f}s "
                        f"after {sent} of {spec.total} records; is it still up?"
                    ) from None
                if waited > 5:
                    log.warning("waiting local queue full for %.0fs", waited)
                producer.poll(0.5)
        sent += 1

        if sent % every == 0:
            producer.poll(0)
            if spec.rate > 0:
                behind = started + sent / spec.rate - time.monotonic()
                if behind > 0:
                    time.sleep(behind)
        if sent % 20_000 == 0:
            elapsed = time.monotonic() - started
            log.info("sent    %7d / %d  (%.0f rec/s)", sent, spec.total, sent / max(elapsed, 1e-9))

    # flush returns how many are still undelivered. Ignoring it is how a demo
    # comes to report a count the table cannot match.
    undelivered = producer.flush(120)
    if undelivered:
        raise RuntimeError(f"{undelivered} record(s) never reached the broker")
    return sent


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="pipeline-load")
    parser.add_argument("--shipments", type=int, default=100_000)
    parser.add_argument(
        "--rate", type=float, default=2_000.0, help="records/second, 0 for no limit"
    )
    parser.add_argument("--match-rate", type=float, default=0.9)
    parser.add_argument("--late-rate", type=float, default=0.02)
    parser.add_argument("--window", default="2 hours", help="event time spanned by the run")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument(
        "--flush",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="close with a far-future matched pair so every unmatched row is released",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    args = parse_args(argv)

    app = AppConfig.load()
    if app.kafka is None:
        log.error("no Kafka configured; set KAFKA_BOOTSTRAP_SERVERS")
        return 1

    spec = LoadSpec(
        shipments=args.shipments,
        rate=args.rate,
        match_rate=args.match_rate,
        late_rate=args.late_rate,
        window=parse_duration(args.window),
        seed=args.seed,
        flush=args.flush,
    )
    log.info(
        "load    %d shipments + %d deliveries over %s of event time, %.0f rec/s",
        spec.shipments,
        spec.deliveries,
        spec.window,
        spec.rate,
    )
    started = time.monotonic()
    sent = run(
        app.kafka,
        spec,
        app.stream.max_lag or timedelta(minutes=30),
        parse_duration(app.stream.lateness),
    )
    elapsed = time.monotonic() - started
    log.info("done    %d records in %.1fs (%.0f rec/s)", sent, elapsed, sent / max(elapsed, 1e-9))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
