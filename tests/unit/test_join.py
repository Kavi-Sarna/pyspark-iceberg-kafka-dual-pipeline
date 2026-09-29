"""What a LEFT JOIN guarantees, and what it does not.

The dataset is the one from Ground School F3, chosen so that a single fixture
covers all four behaviors at once: a clean match, a shipment that never
arrived, a shipment delivered twice, and a delivery for a shipment nobody has
heard of.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.core.join import JoinSpec, left_join
from pipeline.core.schemas import DELIVERY, JOIN_KEY, JOINED, SHIPMENT

SPEC = JoinSpec(keys=(JOIN_KEY,), left_time="dispatch_time", right_time="delivered_time")


def at(minute: int) -> datetime:
    return datetime(2026, 9, 13, 10, minute)


@pytest.fixture
def shipments(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [
            ("shp-1", "carrier-a", at(0)),  # one delivery
            ("shp-2", "carrier-b", at(5)),  # never delivered
            ("shp-3", "carrier-c", at(10)),  # two delivery events
        ],
        SHIPMENT,
    )


@pytest.fixture
def deliveries(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [
            ("dlv-1", "shp-1", at(2)),
            ("dlv-2", "shp-3", at(11)),
            ("dlv-3", "shp-3", at(12)),  # second scan for shp-3
            ("dlv-4", "shp-9", at(20)),  # no such shipment
        ],
        DELIVERY,
    )


def rows(df: DataFrame) -> list[tuple[str, str | None]]:
    ordered = df.orderBy("shipment_id", "delivery_id").collect()
    return [(r["shipment_id"], r["delivery_id"]) for r in ordered]


def test_output_matches_the_declared_schema(shipments: DataFrame, deliveries: DataFrame) -> None:
    """PR 5 declared JOINED. This is the implementation being held to it."""
    actual = left_join(shipments, deliveries, SPEC).schema
    assert [(f.name, f.dataType.simpleString(), f.nullable) for f in actual] == [
        (f.name, f.dataType.simpleString(), f.nullable) for f in JOINED
    ]


def test_the_four_behaviors(shipments: DataFrame, deliveries: DataFrame) -> None:
    assert rows(left_join(shipments, deliveries, SPEC)) == [
        ("shp-1", "dlv-1"),  # matched
        ("shp-2", None),  # never delivered: kept, null-padded
        ("shp-3", "dlv-2"),  # delivered twice: one row per delivery
        ("shp-3", "dlv-3"),
    ]
    # dlv-4 is absent: a left join keeps no right row without a left match.


def test_every_shipment_survives(shipments: DataFrame, deliveries: DataFrame) -> None:
    """The one guarantee: at least one output row per input shipment."""
    out = left_join(shipments, deliveries, SPEC)
    assert out.select(JOIN_KEY).distinct().count() == shipments.count()


def test_row_count_is_not_preserved_when_the_right_side_repeats(
    shipments: DataFrame, deliveries: DataFrame
) -> None:
    """A left join emits one row per matching right row, not one per left row.

    Three shipments in, four rows out. Row count is preserved only when the
    right side is unique on the key, which nothing here enforces.
    """
    assert shipments.count() == 3
    assert left_join(shipments, deliveries, SPEC).count() == 4


def test_unmatched_shipments_are_the_answer(shipments: DataFrame, deliveries: DataFrame) -> None:
    undelivered = left_join(shipments, deliveries, SPEC).filter(F.col("delivery_id").isNull())
    assert [r["shipment_id"] for r in undelivered.collect()] == ["shp-2"]


def test_empty_deliveries_still_emits_every_shipment(
    spark: SparkSession, shipments: DataFrame
) -> None:
    empty = spark.createDataFrame([], DELIVERY)
    out = left_join(shipments, empty, SPEC)
    assert rows(out) == [("shp-1", None), ("shp-2", None), ("shp-3", None)]


def test_empty_shipments_emits_nothing(spark: SparkSession, deliveries: DataFrame) -> None:
    empty = spark.createDataFrame([], SHIPMENT)
    out = left_join(empty, deliveries, SPEC)
    assert out.count() == 0
    assert out.schema == left_join(empty, deliveries, SPEC).schema


def test_max_lag_excludes_a_late_delivery_without_dropping_the_shipment(
    shipments: DataFrame, deliveries: DataFrame
) -> None:
    """A bound inside the condition narrows the match; it never deletes a row.

    shp-3 was dispatched at 10:10 and scanned at 10:11 and 10:12. A one-minute
    window keeps the first and excludes the second, and shp-2 is still here.
    """
    bounded = JoinSpec(
        keys=(JOIN_KEY,),
        left_time="dispatch_time",
        right_time="delivered_time",
        max_lag=timedelta(minutes=1),
    )
    assert rows(left_join(shipments, deliveries, bounded)) == [
        ("shp-1", None),  # delivered at 10:02, two minutes after dispatch
        ("shp-2", None),
        ("shp-3", "dlv-2"),  # 10:11, one minute after dispatch
    ]


def test_a_post_filter_would_turn_this_into_an_inner_join(
    shipments: DataFrame, deliveries: DataFrame
) -> None:
    """Why max_lag lives in the condition rather than in a later .filter().

    For an unmatched shipment the right-hand column is NULL, and NULL > anything
    is NULL rather than true, so the null-padded row is discarded. That deletes
    exactly the rows this job exists to produce.
    """
    joined = left_join(shipments, deliveries, SPEC)
    post_filtered = joined.filter(F.col("delivered_time") > F.col("dispatch_time"))

    assert "shp-2" in [r[0] for r in rows(joined)]
    assert "shp-2" not in [r[0] for r in rows(post_filtered)]
