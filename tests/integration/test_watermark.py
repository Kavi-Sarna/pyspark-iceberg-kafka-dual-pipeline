"""The experiment the streaming design exists to make true.

An unmatched shipment is not emitted when it arrives. It is not lost either.
It waits in state until event time advances far enough that no delivery could
still match it, and only then is it released, null-padded.

File sources rather than Kafka: the point here is the join's semantics, and a
bounded source makes the test deterministic and quick. Kafka's own behaviour is
covered separately.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from pipeline.config import AppConfig
from pipeline.core.join import left_join
from pipeline.core.schemas import DELIVERY, SHIPMENT
from pipeline.runner import stream

BASE = datetime(2026, 9, 13, 10, 0)

# The packages defaults, so the tests and the job cannot disagree about how
# long state is kept or how wide the join window is.
SETTINGS = AppConfig.load(env={}).stream
SETTINGS_SPEC = stream.spec_for(SETTINGS)


def at(minutes: int) -> datetime:
    return BASE + timedelta(minutes=minutes)


class Harness:
    """Two growing directories, one checkpoint, run on demand.

    The parquet sink is used rather than memory because memory cannot recover
    from a checkpoint, and recovering is the entire point: round two has to
    continue the same query rather than start a new one.
    """

    def __init__(self, spark: SparkSession, root: Path) -> None:
        self.spark = spark
        self.root = root
        # Empty frames, only to create the directories a file source needs.
        # A seeded row would be an unmatched shipment of its own and would
        # turn up in every assertion below.
        self.shipments()
        self.deliveries()

    def shipments(self, *rows: tuple[str, str, datetime]) -> None:
        self.spark.createDataFrame(list(rows), SHIPMENT).write.mode("append").parquet(
            str(self.root / "ship")
        )

    def deliveries(self, *rows: tuple[str, str, datetime]) -> None:
        self.spark.createDataFrame(list(rows), DELIVERY).write.mode("append").parquet(
            str(self.root / "dlv")
        )

    def advance(self) -> None:
        """Process everything available, then stop. availableNow bounds it."""
        left = (
            self.spark.readStream.schema(SHIPMENT)
            .parquet(str(self.root / "ship"))
            .dropna()
            .withWatermark("dispatch_time", SETTINGS.lateness)
        )
        right = (
            self.spark.readStream.schema(DELIVERY)
            .parquet(str(self.root / "dlv"))
            .dropna()
            .withWatermark("delivered_time", SETTINGS.lateness)
        )
        query = (
            left_join(left, right, SETTINGS_SPEC)
            .writeStream.format("parquet")
            .outputMode("append")
            .option("path", str(self.root / "out"))
            .option("checkpointLocation", str(self.root / "ck"))
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination(300)

    def emitted(self) -> list[tuple[str, str | None]]:
        try:
            rows = self.spark.read.parquet(str(self.root / "out")).collect()
        except Exception:
            return []
        return sorted((r["shipment_id"], r["delivery_id"]) for r in rows)


@pytest.fixture
def harness(iceberg: SparkSession, tmp_path: Path) -> Harness:
    return Harness(iceberg, tmp_path)


def test_a_matched_pair_is_emitted_without_waiting(harness: Harness) -> None:
    """A match needs no watermark. Both rows are present, so the join is done."""
    harness.shipments(("shp-1", "carrier-a", at(0)))
    harness.deliveries(("dlv-1", "shp-1", at(2)))

    harness.advance()

    assert ("shp-1", "dlv-1") in harness.emitted()


def test_an_unmatched_shipment_is_not_emitted_yet(harness: Harness) -> None:
    """The engine cannot tell 'no delivery yet' from 'no delivery ever'."""
    harness.shipments(("shp-2", "carrier-b", at(5)))

    harness.advance()

    assert harness.emitted() == []


def test_advancing_event_time_releases_it(harness: Harness) -> None:
    """The whole experiment, in one test.

    Round one emits nothing. Round two adds an event far enough ahead on both
    topics, and the shipment that could never match comes out null-padded.
    """
    harness.shipments(("shp-2", "carrier-b", at(5)))
    harness.advance()
    assert harness.emitted() == []

    harness.shipments(("shp-9", "carrier-z", at(300)))
    harness.deliveries(("dlv-9", "shp-9", at(301)))
    harness.advance()

    assert harness.emitted() == [("shp-2", None), ("shp-9", "dlv-9")]


def test_advancing_only_one_side_releases_nothing(harness: Harness) -> None:
    """Why the advance scenario sends to both topics.

    A stream-stream join watermarks on the minimum across its inputs, so a
    quiet delivery topic pins the whole join no matter how far ahead the
    shipments run. This is the usual reason a streaming join looks broken.
    """
    harness.shipments(("shp-2", "carrier-b", at(5)))
    harness.advance()

    harness.shipments(("shp-9", "carrier-z", at(300)))
    harness.advance()

    assert harness.emitted() == []


def test_the_waiting_shipment_is_visible_in_the_state_store(harness: Harness) -> None:
    """Spark 4's state data source: look at the row rather than infer it.

    A pending left row is not a guess about what the engine is doing; it is a
    key and a value sitting in a store you can query like any table.
    """
    harness.shipments(("shp-2", "carrier-b", at(5)))
    harness.deliveries(("dlv-1", "shp-1", at(2)))
    harness.shipments(("shp-1", "carrier-a", at(0)))
    harness.advance()

    left = (
        harness.spark.read.format("statestore")
        .option("path", str(harness.root / "ck"))
        .option("joinSide", "left")
        .load()
    )
    waiting = {r["key"]["field0"] for r in left.collect()}

    assert "shp-2" in waiting


def test_the_state_store_holds_both_sides(harness: Harness) -> None:
    """Both halves are kept, which is what the lateness setting pays for."""
    harness.shipments(("shp-1", "carrier-a", at(0)))
    harness.deliveries(("dlv-1", "shp-1", at(2)))
    harness.advance()

    def side(name: str) -> int:
        return (
            harness.spark.read.format("statestore")
            .option("path", str(harness.root / "ck"))
            .option("joinSide", name)
            .load()
            .count()
        )

    assert side("left") > 0
    assert side("right") > 0
