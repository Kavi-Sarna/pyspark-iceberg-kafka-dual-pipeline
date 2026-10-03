"""The batch path, end to end, against real Iceberg tables.

The order matters and the tests are deliberately sequential: an empty run has
to work before a populated one, because "green against empty tables" is the
property this PR is really claiming.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from pipeline.bootstrap import create_tables
from pipeline.core.schemas import DELIVERY, JOINED, SHIPMENT
from pipeline.io.iceberg import read, table_name
from pipeline.runner import batch

from .conftest import CATALOG

OUTPUT = table_name(CATALOG, batch.NAMESPACE, batch.OUTPUT)


def at(minutes: int) -> datetime:
    return datetime(2026, 9, 13, 10, minutes)


def snapshots(spark: SparkSession, table: str) -> int:
    rows = spark.sql(f"SELECT count(*) AS c FROM {table}.snapshots").collect()
    return int(rows[0]["c"])


@pytest.fixture(scope="session", autouse=True)
def tables(iceberg: SparkSession) -> None:
    create_tables(iceberg, CATALOG)


def test_bootstrap_created_every_table(iceberg: SparkSession) -> None:
    names = {r["tableName"] for r in iceberg.sql(f"SHOW TABLES IN {CATALOG}.ops").collect()}
    assert names == {"shipments", "deliveries", "shipment_deliveries"}


def test_output_table_matches_the_declared_schema(iceberg: SparkSession) -> None:
    """Iceberg stored what PR 5 declared, nullability included."""
    actual = read(iceberg, OUTPUT).schema
    assert [(f.name, f.nullable) for f in actual] == [(f.name, f.nullable) for f in JOINED]


def test_a_run_against_empty_tables_is_green(iceberg: SparkSession) -> None:
    """The whole point of this PR. Nothing to join, and it still succeeds."""
    assert batch.run(iceberg, CATALOG) == 0
    assert read(iceberg, OUTPUT).count() == 0


def test_a_populated_run_writes_the_join(iceberg: SparkSession) -> None:
    iceberg.createDataFrame(
        [
            ("shp-1", "carrier-a", at(0)),
            ("shp-2", "carrier-b", at(5)),
            ("shp-3", "carrier-c", at(10)),
        ],
        SHIPMENT,
    ).writeTo(table_name(CATALOG, batch.NAMESPACE, batch.SHIPMENTS)).append()

    iceberg.createDataFrame(
        [
            ("dlv-1", "shp-1", at(2)),
            ("dlv-2", "shp-3", at(11)),
            ("dlv-3", "shp-3", at(12)),
            ("dlv-4", "shp-9", at(20)),
        ],
        DELIVERY,
    ).writeTo(table_name(CATALOG, batch.NAMESPACE, batch.DELIVERIES)).append()

    assert batch.run(iceberg, CATALOG) == 4

    rows = read(iceberg, OUTPUT).orderBy("shipment_id", "delivery_id").collect()
    assert [(r["shipment_id"], r["delivery_id"]) for r in rows] == [
        ("shp-1", "dlv-1"),
        ("shp-2", None),
        ("shp-3", "dlv-2"),
        ("shp-3", "dlv-3"),
    ]


def test_the_undelivered_shipment_survived_the_round_trip(iceberg: SparkSession) -> None:
    """A null through Parquet and back is the thing most likely to be lost."""
    undelivered = read(iceberg, OUTPUT).filter("delivery_id IS NULL")
    assert [r["shipment_id"] for r in undelivered.collect()] == ["shp-2"]


def test_each_run_adds_a_snapshot(iceberg: SparkSession) -> None:
    """Append is not overwrite: history accumulates, which is time travel.

    Two runs have happened by now, the empty one and the populated one.
    """
    before = snapshots(iceberg, OUTPUT)
    batch.run(iceberg, CATALOG)
    assert snapshots(iceberg, OUTPUT) == before + 1


def test_returning_is_a_fixed_point(iceberg: SparkSession) -> None:
    """Three runs, four rows. overwritePartitions replaces rather than adds.

    With append this held twelve rows by now. The snapshots still accumulate,
    so the history is intact; it is the data that stops growing.
    """
    assert read(iceberg, OUTPUT).count() == 4
    batch.run(iceberg, CATALOG)
    assert read(iceberg, OUTPUT).count() == 4


def test_partitioning_is_hidden(iceberg: SparkSession) -> None:
    """days(dispatch_time) adds no column. That is the whole idea.

    A Hive-style table would need a dispatch_time_day column, every writer
    would have to populate it and every reader would have to filter on it.
    """
    assert read(iceberg, OUTPUT).columns == [f.name for f in JOINED]


def test_the_output_table_is_partitioned_by_day(iceberg: SparkSession) -> None:
    rows = iceberg.sql(f"SELECT partition FROM {OUTPUT}.partitions").collect()
    assert [r["partition"]["dispatch_time_day"] for r in rows] == [date(2026, 9, 13)]


def test_table_properties_were_applied(iceberg: SparkSession) -> None:
    """Properties are strings Iceberg does not validate, so check they stuck."""
    props = dict(
        (r["key"], r["value"]) for r in iceberg.sql(f"SHOW TBLPROPERTIES {OUTPUT}").collect()
    )
    assert props["write.distribution-mode"] == "hash"
    assert props["write.target-file-size-bytes"] == "134217728"


def test_partitioning_prunes_what_an_unclustered_table_cannot(
    iceberg: SparkSession, warehouse: Path
) -> None:
    """The measured case for partitioning, on data that did not arrive sorted.

    Ten days interleaved into one file. The unpartitioned table has a single
    file whose bounds span the whole range, so a one-day filter reads all of
    it. The partitioned table was forced to split by day, so it reads a tenth.
    """
    ddl = SHIPMENT.toDDL()
    iceberg.sql(
        f"CREATE TABLE {CATALOG}.ops.part ({ddl}) USING iceberg "
        f"PARTITIONED BY (days(dispatch_time))"
    )
    iceberg.sql(f"CREATE TABLE {CATALOG}.ops.flat ({ddl}) USING iceberg")

    rows = [
        (f"shp-{i}", "carrier-a", datetime(2026, 9, 1) + timedelta(days=i % 10, minutes=i))
        for i in range(1000)
    ]
    df = iceberg.createDataFrame(rows, SHIPMENT).coalesce(1)
    df.writeTo(f"{CATALOG}.ops.part").append()
    df.writeTo(f"{CATALOG}.ops.flat").append()

    def rows_scanned(table: str) -> int:
        q = (
            f"SELECT * FROM {CATALOG}.ops.{table} "
            f"WHERE dispatch_time >= TIMESTAMP '2026-09-05 00:00:00' "
            f"AND dispatch_time < TIMESTAMP '2026-09-06 00:00:00'"
        )
        d = iceberg.sql(q)
        d.collect()
        leaves = d._jdf.queryExecution().executedPlan().collectLeaves().iterator()
        while leaves.hasNext():
            metrics = leaves.next().metrics()
            if metrics.contains("numOutputRows"):
                return int(metrics.apply("numOutputRows").value())
        raise AssertionError("No Scan metric found")

    assert rows_scanned("flat") == 1000
    assert rows_scanned("part") == 100
