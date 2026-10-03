"""Batch: Iceberg in, join, Iceberg out."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from pipeline.core.join import JoinSpec, left_join
from pipeline.core.schemas import JOIN_KEY
from pipeline.io.iceberg import overwrite, read, table_name

log = logging.getLogger("pipeline.batch")

NAMESPACE = "ops"
SHIPMENTS = "shipments"
DELIVERIES = "deliveries"
OUTPUT = "shipment_deliveries"

# Batch needs no time bound. The streaming runner supplies one, because a
# stream-stream left outer join will not plan without it.
SPEC = JoinSpec(keys=(JOIN_KEY,), left_time="dispatch_time", right_time="delivered_time")


def run(spark: SparkSession, catalog: str) -> int:
    """Join the two tables and append the result. Returns the rows written."""
    shipments = read(spark, table_name(catalog, NAMESPACE, SHIPMENTS))
    deliveries = read(spark, table_name(catalog, NAMESPACE, DELIVERIES))

    joined = left_join(shipments, deliveries, SPEC)
    joined.cache()
    try:
        written = joined.count()
        # Replaces only the days this run produced, so re-running is a fixed
        # point rather than a doubling. Days absent from the input are left
        # alone, whichis why the output is partitioned at all.
        overwrite(joined, table_name(catalog, NAMESPACE, OUTPUT))
    finally:
        joined.unpersist()

    log.info("Wrote %d rows to %s", written, table_name(catalog, NAMESPACE, OUTPUT))
    return written
