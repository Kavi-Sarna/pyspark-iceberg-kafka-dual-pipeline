"""Batch: Iceberg in, join, Iceberg out."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from pipeline.config import Tables
from pipeline.core.join import JoinSpec, left_join
from pipeline.core.schemas import JOIN_KEY
from pipeline.io.iceberg import overwrite, read, table_name

log = logging.getLogger("pipeline.batch")

# Batch needs no time bound. The streaming runner supplies one, because a
# stream-stream left outer join will not plan without it.
SPEC = JoinSpec(keys=(JOIN_KEY,), left_time="dispatch_time", right_time="delivered_time")


def run(spark: SparkSession, catalog: str, tables: Tables) -> int:
    """Join the two tables and append the result. Returns the rows written."""
    shipments = read(spark, table_name(catalog, tables.namespace, tables.shipments))
    deliveries = read(spark, table_name(catalog, tables.namespace, tables.deliveries))

    target = table_name(catalog, tables.namespace, tables.output)
    joined = left_join(shipments, deliveries, SPEC)
    joined.cache()
    try:
        written = joined.count()
        # Replaces only the days this run produced, so re-running is a fixed
        # point rather than a doubling. Days absent from the input are left
        # alone, which is why the output is partitioned at all.
        overwrite(joined, target)
    finally:
        joined.unpersist()

    log.info("wrote   %d rows to %s", written, target)
    return written
