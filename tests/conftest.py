"""A live SparkSession for any test that needs one.

Deliberately not session-scoped. Spark allows one context per JVM, and the
tests for main() stop the session they build - a cached fixture would hand
later tests a stopped object, failing with an AttributeError deep inside
pyspark rather than anything that names the cause.

getOrCreate returns the active session when there is one and builds a new one
when the last was stopped, so this stays cheap without going stale.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("pipeline-tests")
        # Default is 200, so every shuffle would schedule 200 near-empty tasks.
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")
    return session


@pytest.fixture(scope="session", autouse=True)
def _stop_session_at_exit() -> Iterator[None]:
    yield
    active = SparkSession.getActiveSession()
    if active is not None:
        active.stop()
