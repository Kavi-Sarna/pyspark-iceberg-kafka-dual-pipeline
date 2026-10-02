"""Create the namespace and the tables the pipeline reads and writes.

The DDL comes from the declared schema rather than a second copy of the column
list, so the tables cannot drift from what the code expects.
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from pipeline.core.schemas import DELIVERY, JOINED, SHIPMENT
from pipeline.session import CatalogConfig, build_session

log = logging.getLogger("pipeline.bootstrap")

NAMESPACE = "ops"
TABLES: dict[str, StructType] = {
    "shipments": SHIPMENT,
    "deliveries": DELIVERY,
    "shipment_deliveries": JOINED,
}


def create_tables(spark: SparkSession, catalog: str) -> None:
    # base_warehouse = warehouse.rstrip("/")

    # namespace_location = f"{base_warehouse}/{NAMESPACE}"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{NAMESPACE}")
    for table, schema in TABLES.items():
        fqn = f"{catalog}.{NAMESPACE}.{table}"
        # table_location = f"{namespace_location}/{table}"
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {fqn} ({schema.toDDL()}) USING iceberg"
            # f"LOCATION '{table_location}'"
        )
        log.info("ready   %s", fqn)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s")

    catalog = CatalogConfig.from_env()
    if catalog is None:
        log.error("no catalog configured; set ICEBERG_WAREHOUSE")
        return 1

    spark = build_session("pipeline-bootstrap", catalog=catalog)
    spark.sparkContext.setLogLevel("WARN")
    try:
        create_tables(spark, catalog.name)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
