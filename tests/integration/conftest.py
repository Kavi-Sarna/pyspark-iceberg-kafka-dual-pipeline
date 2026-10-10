"""A SparkSession with the Iceberg jars, backed by a directory catalog.

These live apart from the unit tests for one hard reason: `spark.jars.packages`
is read when the JVM launches, and there is one JVM per process. If a plain
session has already been created, `getOrCreate` hands it back without Iceberg
on the classpath and every test here fails with ClassNotFoundException. So this
directory must run in its own pytest process - `make test-integration` does
that, and `ci/run.sh` invokes the two separately.

No Docker and no object storage: a `hadoop` catalog is a directory, which is
enough to exercise the whole read/join/write path.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from pipeline.config import AppConfig, StreamSettings, Tables
from pipeline.session import STREAM_PACKAGES, CatalogConfig, build_session

CATALOG = "lake"


@pytest.fixture(scope="session")
def warehouse(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("warehouse")


@pytest.fixture(scope="session")
def iceberg(warehouse: Path) -> Iterator[SparkSession]:
    if SparkSession.getActiveSession() is not None:
        pytest.fail(
            "a SparkSession already exists, so the Iceberg jars cannot be added. "
            "Run the integration tests in their own process."
        )
    session = build_session(
        "pipeline-integration",
        catalog=CatalogConfig(name=CATALOG, type="hadoop", warehouse=str(warehouse)),
        # from_avro lives in spark-avro, which the pyspark wheel does not ship.
        extra_packages=(STREAM_PACKAGES,),
    )
    session.sparkContext.setLogLevel("WARN")
    session.conf.set("spark.sql.shuffle.partitions", "1")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def tables() -> Tables:
    """The packages defaults, so tests and the job agree on table names."""
    return AppConfig.load(env={}).tables


@pytest.fixture(scope="session")
def settings() -> StreamSettings:
    return AppConfig.load(env={}).stream
