"""Watch an unmatched shipment wait, then be released.

The same experiment the integration tests assert, printed as it happens. File
sources rather than Kafka, so it needs no Docker and finishes in under a
minute.

It uses the streaming runner's own LATENESS and SPEC, so it cannot drift from
what actually runs.
"""

from __future__ import annotations

import argparse
import logging
import shutil
from collections.abc import Sequence
from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import SparkSession

from pipeline.config import AppConfig, StreamSettings
from pipeline.core.join import left_join
from pipeline.core.schemas import DELIVERY, SHIPMENT
from pipeline.runner import stream
from pipeline.session import build_session

log = logging.getLogger("pipeline.watermark")

BASE = datetime(2026, 9, 13, 10, 0)


def at(minutes: int) -> datetime:
    return BASE + timedelta(minutes=minutes)


class Demo:
    def __init__(self, spark: SparkSession, root: Path, settings: StreamSettings) -> None:
        self.spark = spark
        self.root = root
        self.settings = settings
        # Empty frames only, to create the directories a file source needs.
        self.add_shipments()
        self.add_deliveries()

    def add_shipments(self, *rows: tuple[str, str, datetime]) -> None:
        self.spark.createDataFrame(list(rows), SHIPMENT).write.mode("append").parquet(
            str(self.root / "shipments")
        )

    def add_deliveries(self, *rows: tuple[str, str, datetime]) -> None:
        self.spark.createDataFrame(list(rows), DELIVERY).write.mode("append").parquet(
            str(self.root / "deliveries")
        )

    def advance(self) -> None:
        """Process everything available and stop, continuing the same query."""
        shipments = (
            self.spark.readStream.schema(SHIPMENT)
            .parquet(str(self.root / "shipments"))
            .dropna()
            .withWatermark("dispatch_time", self.settings.lateness)
        )
        deliveries = (
            self.spark.readStream.schema(DELIVERY)
            .parquet(str(self.root / "deliveries"))
            .dropna()
            .withWatermark("delivered_time", self.settings.lateness)
        )
        query = (
            left_join(shipments, deliveries, stream.spec_for(self.settings))
            .writeStream.format("parquet")
            .outputMode("append")
            .option("path", str(self.root / "out"))
            .option("checkpointLocation", str(self.root / "checkpoint"))
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination(300)

    def report(self, label: str) -> None:
        try:
            rows = self.spark.read.parquet(str(self.root / "out")).collect()
        except Exception:
            rows = []
        emitted = sorted((r["shipment_id"], r["delivery_id"]) for r in rows)
        print(f"\n--- {label} ---")
        print(f"  emitted     : {emitted or 'nothing'}")
        for side in ("left", "right"):
            keys = sorted(
                r["key"]["field0"]
                for r in self.spark.read.format("statestore")
                .option("path", str(self.root / "checkpoint"))
                .option("joinSide", side)
                .load()
                .collect()
            )
            print(f"  state {side:5} : {keys or 'empty'}")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="pipeline-watermark-demo")
    parser.add_argument("--dir", default=".watermark-demo", help="scratch directory")
    parser.add_argument("--keep", action="store_true", help="do not delete it first")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.WARN)
    args = parse_args(argv)

    root = Path(args.dir).resolve()
    if not args.keep and root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)

    spark = build_session("pipeline-watermark-demo")
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    try:
        settings = AppConfig.load().stream
        demo = Demo(spark, root, settings)

        print(f"\nlateness {settings.lateness}, bound {settings.max_lag}")

        demo.add_shipments(("shp-2", "carrier-b", at(5)))
        demo.advance()
        demo.report("a shipment with no delivery")
        print("  shp-2 is held: nothing distinguishes 'not yet' from 'never'.")

        demo.add_shipments(("shp-9", "carrier-z", at(300)))
        demo.add_deliveries(("dlv-9", "shp-9", at(301)))
        demo.advance()
        demo.report("after advancing event time on both topics")
        print("  shp-2 is released null-padded, and has left the state store.")
        print("  Advancing only one topic would have released nothing: the")
        print("  watermark is the minimum across inputs.\n")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
