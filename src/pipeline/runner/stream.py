"""Streaming: Kafka in, join, Iceberg out."""

from __future__ import annotations

import logging
from datetime import timedelta

from pyspark.sql import SparkSession
from pyspark.sql.streaming.query import StreamingQuery

from pipeline.core.join import JoinSpec, left_join
from pipeline.core.schemas import JOIN_KEY
from pipeline.io.iceberg import table_name
from pipeline.io.kafka import read_stream
from pipeline.runner.batch import NAMESPACE, OUTPUT
from pipeline.streams import DELIVERIES, SHIPMENTS, StreamConfig

log = logging.getLogger("pipeline.stream")

# How late an event may arrive and still be joined. Also how long state is
# kept, so it is a memory decision as much as a correctness one.
LATENESS = "10 minutes"

# The difference from batch, as data rather than a branch. A stream-stream
# left outer join will not plan without a time-range bound; watermarks on both
# sides are necessary and not sufficient.
SPEC = JoinSpec(
    keys=(JOIN_KEY,),
    left_time="dispatch_time",
    right_time="delivered_time",
    max_lag=timedelta(minutes=30),
)

TRIGGER = "10 seconds"


def run(spark: SparkSession, catalog: str, cfg: StreamConfig, checkpoint: str) -> StreamingQuery:
    """Start the query and return it. The caller decides how long to wait."""
    # Iceberg refuses a nullable dataset into a required column, and a stream
    # is always nullable. read_stream drops any row with a null, so the claim
    # this disables checking for is one we have already enforced. Disabling it
    # without that would just move the lie into the table.
    spark.conf.set("spark.sql.iceberg.check-nullability", "false")

    shipments = read_stream(spark, cfg, SHIPMENTS, "shipment", "dispatch_time", LATENESS)
    deliveries = read_stream(spark, cfg, DELIVERIES, "delivery", "delivered_time", LATENESS)

    joined = left_join(shipments, deliveries, SPEC)
    target = table_name(catalog, NAMESPACE, OUTPUT)

    log.info("stream  %s + %s -> %s", SHIPMENTS, DELIVERIES, target)
    log.info("state   lateness %s, bound %s", LATENESS, SPEC.max_lag)

    return (
        joined.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=TRIGGER)
        .toTable(target)
    )
