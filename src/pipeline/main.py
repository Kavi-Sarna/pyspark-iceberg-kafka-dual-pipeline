"""Entry point: parse a mode and delegate."""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence

from pipeline.config import AppConfig
from pipeline.metrics import MetricsListener, Statsd
from pipeline.runner import batch, stream
from pipeline.session import STREAM_PACKAGES, CatalogConfig, build_session

log = logging.getLogger("pipeline")

MODES = ("report", "batch", "stream")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="pipeline")
    parser.add_argument("--mode", choices=MODES, default="report")
    parser.add_argument("--config", help="TOML file overriding the packaged defaults")
    parser.add_argument(
        "--await-seconds",
        type=int,
        default=0,
        help="0 runs until interrupted; a positive value stops after that long",
    )
    return parser.parse_args(argv)


def report(spark: object, catalog: CatalogConfig | None) -> None:
    log.info("spark   %s", getattr(spark, "version", "?"))
    log.info("python  %s", sys.version.split()[0])
    log.info("catalog %s", f"{catalog.name} ({catalog.type})" if catalog else "none")


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    args = parse_args(argv)

    app = AppConfig.load(args.config)
    catalog, streams = app.catalog, app.kafka

    if args.mode in ("batch", "stream") and catalog is None:
        log.error("%s mode needs a catalog; set ICEBERG_WAREHOUSE", args.mode)
        return 1
    if args.mode == "stream" and streams is None:
        log.error("stream mode needs Kafka; set KAFKA_BOOTSTRAP_SERVERS")
        return 1

    # The Kafka and Avro jars are only fetched when streaming, so a batch run
    # does not pay for them.
    extra = (STREAM_PACKAGES,) if args.mode == "stream" else ()
    spark = build_session(
        catalog=catalog, extra_packages=extra, event_options=app.events.spark_options()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        log.info("master  %s", spark.sparkContext.master)
        if args.mode == "batch":
            assert catalog is not None
            batch.run(spark, catalog.name, app.tables)
        elif args.mode == "stream":
            assert catalog is not None and streams is not None
            if app.metrics.enabled:
                # Registered on the session, so it sees every query, and
                # before start so the first trigger is not missed.
                spark.streams.addListener(MetricsListener(Statsd(app.metrics)))
                log.info("metrics %s:%s", app.metrics.host, app.metrics.port)
            query = stream.run(spark, catalog.name, streams, app.tables, app.stream)
            if args.await_seconds:
                query.awaitTermination(args.await_seconds)
                query.stop()
            else:
                query.awaitTermination()
        else:
            report(spark, catalog)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
