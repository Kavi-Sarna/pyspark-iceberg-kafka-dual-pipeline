"""One SparkSession for the whole test run.

JVM startup dominates these tests, so a session-scoped fixture is the
difference between seconds and minutes.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("pipeline-tests")
        # Default is 200, so every shuffle would schedule 200 near-empty tasks.
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")
    yield session
    session.stop()