"""Create the namespace and the tables the pipeline reads and writes.

The DDL comes from the declared schema rather than a second copy of the column
list, so the tables cannot drift from what the code expects.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from pipeline.core.schemas import DELIVERY, JOINED, SHIPMENT
from pipeline.session import CatalogConfig, build_session
from pipeline.streams import StreamConfig
from pipeline.streams import setup as setup_streams

log = logging.getLogger("pipeline.bootstrap")

NAMESPACE = "ops"

# hash: shuffle by partition key before writing, so each partition gets one
# writer instead of one per Spark task. Without it a 200-task job writing 3
# days produces up to 600 files.
# target-file-size: what a rewrite aims for, and the point writers roll over.
PROPERTIES: Mapping[str, str] = {
    "write.distribution-mode": "hash",
    "write.target-file-size-bytes": "134217728",
    "write.parquet.compression-codec": "zstd",
}


@dataclass(frozen=True)
class TableSpec:
    schema: StructType
    partition_by: str
    properties: Mapping[str, str] = field(default_factory=lambda: PROPERTIES)


# Partitioned on the event time that is never null. The output uses
# dispatch_time, not delivered_time, which is null for exactly the rows this
# job exists to find and would collect them all into one null partition.
TABLES: Mapping[str, TableSpec] = {
    "shipments": TableSpec(SHIPMENT, "days(dispatch_time)"),
    "deliveries": TableSpec(DELIVERY, "days(delivered_time)"),
    "shipment_deliveries": TableSpec(JOINED, "days(dispatch_time)"),
}


def create_table_sql(fqn: str, columns: str, spec: TableSpec) -> str:
    """The CREATE TABLE statement, as a string so it can be tested."""
    properties = ", ".join(f"'{k}'='{v}'" for k, v in sorted(spec.properties.items()))
    return (
        f"CREATE TABLE IF NOT EXISTS {fqn} ({columns}) USING iceberg "
        f"PARTITIONED BY ({spec.partition_by}) "
        f"TBLPROPERTIES ({properties})"
    )


def create_tables(spark: SparkSession, catalog: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{NAMESPACE}")
    for table, spec in TABLES.items():
        fqn = f"{catalog}.{NAMESPACE}.{table}"
        spark.sql(create_table_sql(fqn, spec.schema.toDDL(), spec))
        log.info("ready   %s", fqn)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s")

    catalog = CatalogConfig.from_env()
    streams = StreamConfig.from_env()
    if catalog is None and streams is None:
        log.error("nothing configured; set ICEBERG_WAREHOUSE or KAFKA_BOOTSTRAP_SERVERS")
        return 1

    # Kafka first: it needs no Spark, so a Kafka-only bootstrap never pays for
    # a JVM.
    if streams is not None:
        setup_streams(streams)

    if catalog is not None:
        spark = build_session("pipeline-bootstrap", catalog=catalog)
        spark.sparkContext.setLogLevel("WARN")
        try:
            create_tables(spark, catalog.name)
        finally:
            spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
