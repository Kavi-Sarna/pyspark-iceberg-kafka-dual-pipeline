"""Streaming: Kafka in, join, Iceberg out."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql.streaming.query import StreamingQuery

from pipeline.config import StreamSettings, Tables
from pipeline.core.join import JoinSpec, left_join
from pipeline.core.schemas import JOIN_KEY
from pipeline.io.iceberg import table_name
from pipeline.io.kafka import read_stream
from pipeline.streams import DELIVERIES, SHIPMENTS, StreamConfig

log = logging.getLogger("pipeline.stream")


def spec_for(settings: StreamSettings) -> JoinSpec:
    """The difference from batch, as data rather than a branch.

    A stream-stream left outer join will not plan without a time-range bound;
    watermarks on both sides are necessary and not sufficient.
    """
    return JoinSpec(
        keys=(JOIN_KEY,),
        left_time="dispatch_time",
        right_time="delivered_time",
        max_lag=settings.max_lag,
    )


def run(
    spark: SparkSession, catalog: str, cfg: StreamConfig, tables: Tables, settings: StreamSettings
) -> StreamingQuery:
    """Start the query and return it. The caller decides how long to wait."""
    # Iceberg refuses a nullable dataset into a required column, and a stream
    # is always nullable. read_stream drops any row with a null, so the claim
    # this disables checking for is one we have already enforced. Disabling it
    # without that would just move the lie into the table.
    spark.conf.set("spark.sql.iceberg.check-nullability", "false")

    lateness = settings.lateness
    shipments = read_stream(spark, cfg, SHIPMENTS, "shipment", "dispatch_time", lateness)
    deliveries = read_stream(spark, cfg, DELIVERIES, "delivery", "delivered_time", lateness)

    spec = spec_for(settings)
    joined = left_join(shipments, deliveries, spec)
    target = table_name(catalog, tables.namespace, tables.output)

    log.info("stream  %s + %s -> %s", SHIPMENTS, DELIVERIES, target)
    log.info("state   lateness %s, bound %s", lateness, spec.max_lag)

    return (
        joined.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", settings.checkpoint)
        .trigger(processingTime=settings.trigger)
        .toTable(target)
    )
