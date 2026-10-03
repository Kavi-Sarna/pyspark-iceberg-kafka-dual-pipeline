"""Put the worked example into the input tables.

The dataset is the one from the Ground School exercise and the join tests: one
shipment delivered once, one never delivered, one delivered twice, and a
delivery for a shipment that does not exist. Small enough to reason about,
broad enough to show every LEFT JOIN behavior.
"""

from __future__ import annotations

import logging
from datetime import datetime

from pyspark.sql import SparkSession

from pipeline.core.schemas import DELIVERY, SHIPMENT
from pipeline.io.iceberg import replace, table_name
from pipeline.runner import batch
from pipeline.session import CatalogConfig, build_session

log = logging.getLogger("pipeline.seed")


def at(minute: int) -> datetime:
    return datetime(2026, 9, 13, 10, minute)


SHIPMENTS = [
    ("shp-1", "carrier-a", at(0)),  # delivered once
    ("shp-2", "carrier-b", at(5)),  # never delivered
    ("shp-3", "carrier-c", at(10)),  # delivered twice
]
DELIVERIES = [
    ("dlv-1", "shp-1", at(2)),
    ("dlv-2", "shp-3", at(11)),
    ("dlv-3", "shp-3", at(12)),
    ("dlv-4", "shp-9", at(20)),  # no such shipment
]


def seed(spark: SparkSession, catalog: str) -> None:
    replace(
        spark.createDataFrame(SHIPMENTS, SHIPMENT),
        table_name(catalog, batch.NAMESPACE, batch.SHIPMENTS),
    )
    replace(
        spark.createDataFrame(DELIVERIES, DELIVERY),
        table_name(catalog, batch.NAMESPACE, batch.DELIVERIES),
    )
    log.info("seeded %d shipments, %d deliveries", len(SHIPMENTS), len(DELIVERIES))


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    catalog = CatalogConfig.from_env()
    if catalog is None:
        log.error("no catalog configures; set ICEBERG_WAREHOUSE")
        return 1

    spark = build_session("pipeline-seed", catalog=catalog)
    spark.sparkContext.setLogLevel("WARN")
    try:
        seed(spark, catalog.name)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
