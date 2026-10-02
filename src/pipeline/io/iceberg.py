"""Reading and writing Iceberg tables.

Thin on purpose: the core must never import this, and this must never contain
domain logic.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def table_name(catalog: str, namespace: str, table: str) -> str:
    return f"{catalog}.{namespace}.{table}"


def read(spark: SparkSession, table: str) -> DataFrame:
    """Read a table by name.

    No schema is passed: Iceberg stores it, so the table is the declaration.
    That is the difference from reading files, where inference would guess.
    """
    return spark.table(table)


def append(df: DataFrame, table: str) -> None:
    """Add a snapshot. Existing data is untouched."""
    df.writeTo(table).append()


def overwrite(df: DataFrame, table: str) -> None:
    """Replace the table contents in one atomic snapshot.

    A reader either sees all the old rows or all the new ones, never a mixture,
    because the swap is a metadata pointer move.
    """
    df.writeTo(table).overwritePartitions()
