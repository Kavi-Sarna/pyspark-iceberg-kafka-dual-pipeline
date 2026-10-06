"""The streaming join's rules, and a real micro-batch into Iceberg.

No Kafka here. The rate source produces streaming DataFrames shaped like the
two events, which is enough to exercise everything that is actually hard: what
the planner will and will not accept, and whether a streaming write to an
Iceberg table works at all. Kafka itself arrives with testcontainers.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from pipeline.core.join import JoinSpec, left_join
from pipeline.core.schemas import JOIN_KEY
from pipeline.io.kafka import HEADER_BYTES, decode
from pipeline.producer import at, encode
from pipeline.runner import stream
from pipeline.streams import load_schema

from .conftest import CATALOG

LATENESS = "10 minutes"


def events(spark: SparkSession, time_column: str) -> DataFrame:
    """A stream shaped like one of our events, from the rate source."""
    return (
        spark.readStream.format("rate")
        .option("rowsPerSecond", 5)
        .load()
        .select(
            F.concat(F.lit("shp-"), (F.col("value") % 3).cast("string")).alias(JOIN_KEY),
            F.lit("carrier-a").alias("carrier"),
            F.col("timestamp").alias(time_column),
        )
        .withWatermark(time_column, LATENESS)
    )


def test_decode_strips_exactly_five_bytes(iceberg: SparkSession) -> None:
    """The adapter must undo what the producer did, byte for byte."""
    raw = encode(
        {"shipment_id": "shp-1", "carrier": "carrier-a", "dispatch_time": at(0)},
        load_schema("shipment"),
        registry_id=1,
    )
    frame = iceberg.createDataFrame([(bytearray(raw),)], "value binary")

    row = decode(frame, load_schema("shipment")).collect()[0]

    assert HEADER_BYTES == 5
    assert row["shipment_id"] == "shp-1"
    assert row["carrier"] == "carrier-a"


def test_decoding_without_stripping_corrupts_silently(iceberg: SparkSession) -> None:
    """Forgetting the five bytes does not raise. That is the dangerous part.

    Avro reads the framing as the start of the record, so every string comes
    back empty and the timestamp comes back as the epoch. A pipeline wired
    this way runs clean, writes rows, and every one of them is blank.
    """
    raw = encode(
        {"shipment_id": "shp-1", "carrier": "carrier-a", "dispatch_time": at(0)},
        load_schema("shipment"),
        registry_id=1,
    )
    frame = iceberg.createDataFrame([(bytearray(raw),)], "value binary")

    row = frame.select(
        from_avro(F.col("value"), json.dumps(load_schema("shipment"))).alias("r")
    ).collect()[0]["r"]

    assert row["shipment_id"] == ""
    assert row["carrier"] == ""
    # Spark returns a naive datetime in the session timezone; the epoch is the
    # timezone-independent way to say "all zero bytes".
    assert row["dispatch_time"].timestamp() == 0.0


def test_left_outer_needs_more_than_watermarks(iceberg: SparkSession) -> None:
    """Both sides watermarked and it still will not plan.

    This is the rule people get half right: the watermarks are necessary and
    the time-range bound is the other half.
    """
    left = events(iceberg, "dispatch_time")
    right = events(iceberg, "delivered_time")

    unbounded = JoinSpec(keys=(JOIN_KEY,), left_time="dispatch_time", right_time="delivered_time")
    with pytest.raises(Exception, match="(?i)watermark|time"):
        left_join(left, right, unbounded).writeStream.format("noop").start()


def test_the_bounded_spec_plans(iceberg: SparkSession) -> None:
    """The same join, with the streaming runner's own spec, is accepted."""
    left = events(iceberg, "dispatch_time")
    right = events(iceberg, "delivered_time")

    query = left_join(left, right, stream.SPEC).writeStream.format("noop").start()
    try:
        assert query.isActive
    finally:
        query.stop()


def test_the_batch_spec_is_the_one_without_a_bound(iceberg: SparkSession) -> None:
    """The mode difference is data. Nothing branches on it."""
    from pipeline.runner import batch

    assert batch.SPEC.max_lag is None
    assert stream.SPEC.max_lag is not None
    assert batch.SPEC.keys == stream.SPEC.keys


def test_streaming_sources_are_always_nullable(iceberg: SparkSession, tmp_path: Path) -> None:
    """Not an Avro quirk. A file source handed a non-nullable schema still
    reports every column nullable, which is why the adapter has to enforce it
    and the Iceberg write has to be told."""
    schema = StructType(
        [
            StructField("a", StringType(), nullable=False),
            StructField("ts", TimestampType(), nullable=False),
        ]
    )
    iceberg.createDataFrame([("x", datetime(2026, 9, 13, 10, 0))], schema).write.parquet(
        str(tmp_path / "in")
    )
    source = iceberg.readStream.schema(schema).parquet(str(tmp_path / "in"))
    assert [f.nullable for f in source.schema.fields] == [True, True]


def test_a_streaming_write_reaches_a_required_table(iceberg: SparkSession, tmp_path: Path) -> None:
    """The combination that makes it work: enforce, then stop checking.

    availableNow bounds the source, so the query terminates. processAllAvailable
    on an unbounded source such as rate never returns.
    """
    iceberg.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.stream_test")
    target = f"{CATALOG}.stream_test.rows"
    iceberg.sql(
        f"CREATE TABLE IF NOT EXISTS {target} "
        f"(a STRING NOT NULL, ts TIMESTAMP NOT NULL) USING iceberg"
    )
    schema = StructType(
        [
            StructField("a", StringType(), nullable=False),
            StructField("ts", TimestampType(), nullable=False),
        ]
    )
    iceberg.createDataFrame(
        [("x", datetime(2026, 9, 13, 10, 0)), ("y", datetime(2026, 9, 13, 11, 0))],
        schema,
    ).write.parquet(str(tmp_path / "in"))

    iceberg.conf.set("spark.sql.iceberg.check-nullability", "false")
    query = (
        iceberg.readStream.schema(schema)
        .parquet(str(tmp_path / "in"))
        .dropna()
        .writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", str(tmp_path / "ckpt"))
        .trigger(availableNow=True)
        .toTable(target)
    )
    query.awaitTermination(120)

    assert iceberg.table(target).count() == 2
