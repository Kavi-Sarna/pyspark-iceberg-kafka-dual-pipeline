"""The schemas are a contract, so assert them field by field.

A test that only checks column names passes while a timestamp silently becomes a
string, or while a field that must never be null is declared nullable.
"""

from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from pipeline.core.schemas import DELIVERY, JOIN_KEY, JOINED, SHIPMENT

Field = tuple[str, str, bool]


def fields(schema: StructType) -> list[Field]:
    return [(f.name, f.dataType.simpleString(), f.nullable) for f in schema.fields]


@pytest.mark.parametrize(
    ("schema", "expected"),
    [
        (
            SHIPMENT,
            [
                ("shipment_id", "string", False),
                ("carrier", "string", False),
                ("dispatch_time", "timestamp", False),
            ],
        ),
        (
            DELIVERY,
            [
                ("delivery_id", "string", False),
                ("shipment_id", "string", False),
                ("delivered_time", "timestamp", False),
            ],
        ),
        (
            JOINED,
            [
                ("shipment_id", "string", False),
                ("carrier", "string", False),
                ("dispatch_time", "timestamp", False),
                ("delivery_id", "string", True),
                ("delivered_time", "timestamp", True),
            ],
        ),
    ],
    ids=["shipment", "delivery", "joined"],
)
def test_schema_is_exactly_as_declared(schema: StructType, expected: list[Field]) -> None:
    assert fields(schema) == expected


def test_join_key_is_present_on_both_sides() -> None:
    assert JOIN_KEY in SHIPMENT.fieldNames()
    assert JOIN_KEY in DELIVERY.fieldNames()


def test_left_join_produces_the_declared_output_schema(spark: SparkSession) -> None:
    """JOINED is a claim about what a left join yields. Check it against one.

    The join itself arrives in the next PR; this asserts the declaration is
    right, so that implementation has a schema to be measured against.
    """
    shipments = spark.createDataFrame(
        [("shp-1", "carrier-a", datetime(2026, 9, 13, 10, 0))], SHIPMENT
    )
    deliveries = spark.createDataFrame([("dlv-1", "shp-1", datetime(2026, 9, 13, 10, 2))], DELIVERY)

    joined = shipments.join(deliveries, on=JOIN_KEY, how="left")

    assert fields(joined.schema) == fields(JOINED)


def test_left_join_relaxes_nullability_only_on_the_right(spark: SparkSession) -> None:
    """Nullability here is a property of the join type, not of the data.

    Both inputs declare every field non-null and neither row is null, yet the
    right-hand fields come back nullable from a left join and non-null from an
    inner one.
    """
    shipments = spark.createDataFrame(
        [("shp-1", "carrier-a", datetime(2026, 9, 13, 10, 0))], SHIPMENT
    )
    deliveries = spark.createDataFrame([("dlv-1", "shp-1", datetime(2026, 9, 13, 10, 2))], DELIVERY)

    left = {f.name: f.nullable for f in shipments.join(deliveries, JOIN_KEY, "left").schema}
    inner = {f.name: f.nullable for f in shipments.join(deliveries, JOIN_KEY, "inner").schema}

    assert left["delivery_id"] is True
    assert left["delivered_time"] is True
    assert left["carrier"] is False

    assert inner["delivery_id"] is False
    assert inner["delivered_time"] is False


def test_non_nullable_is_enforced_not_decorative(spark: SparkSession) -> None:
    """Declaring nullable=False makes Spark reject the row, not coerce it."""
    with pytest.raises(Exception, match="FIELD_NOT_NULLABLE"):
        spark.createDataFrame([("shp-2", None, datetime(2026, 9, 13, 10, 0))], SHIPMENT).collect()
