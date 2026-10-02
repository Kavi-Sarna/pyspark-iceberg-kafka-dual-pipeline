"""The batch path, end to end, against real Iceberg tables.

The order matters and the tests are deliberately sequential: an empty run has
to work before a populated one, because "green against empty tables" is the
property this PR is really claiming.
"""

from __future__ import annotations

from datetime import datetime

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


def test_appending_twice_duplicates_rows(iceberg: SparkSession) -> None:
    """Worth asserting so nobody mistakes append for idempotent.

    The previous test ran the job a third time, so the output now holds the
    join twice over. Making this a fixed point is PR 9's problem.
    """
    assert read(iceberg, OUTPUT).count() == 8
