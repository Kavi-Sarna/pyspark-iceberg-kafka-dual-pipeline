"""The CREATE TABLE statement, checked without a JVM.

Iceberg does not validate property names: a misspelled one is accepted and
silently does nothing. Pinning the statement is the cheapest way to notice.
"""

from __future__ import annotations

from pipeline.bootstrap import PROPERTIES, TABLES, TableSpec, create_table_sql
from pipeline.core.schemas import SHIPMENT


def test_statement_has_partitioning_and_properties() -> None:
    sql = create_table_sql(
        "lake.ops.shipments",
        "shipment_id STRING NOT NULL",
        TableSpec(SHIPMENT, "days(dispatch_time)"),
    )
    assert sql == (
        "CREATE TABLE IF NOT EXISTS lake.ops.shipments (shipment_id STRING NOT NULL) "
        "USING iceberg "
        "PARTITIONED BY (days(dispatch_time)) "
        "TBLPROPERTIES ("
        "'write.distribution-mode'='hash', "
        "'write.parquet.compression-codec'='zstd', "
        "'write.target-file-size-bytes'='134217728')"
    )


def test_if_not_exists_so_the_bootstrap_can_be_rerun() -> None:
    sql = create_table_sql("t", "a INT", TableSpec(SHIPMENT, "days(x)"))
    assert "IF NOT EXISTS" in sql


def test_every_table_is_repartitioned_by_a_non_null_column() -> None:
    """delivered_time is null for undelivered shipments, so the output must
    not partition on it: every such row would land in one null partition."""
    assert TABLES["shipment_deliveries"].partition_by == "days(dispatch_time)"
    assert TABLES["shipments"].partition_by == "days(dispatch_time)"
    assert TABLES["deliveries"].partition_by == "days(delivered_time)"


def test_distribution_mode_is_set() -> None:
    """Without it, each Spark task writes its own file per partition."""
    assert PROPERTIES["write.distribution-mode"] == "hash"
