"""Catalog configuration is pure, so it can be tested without a JVM.

Every one of these keys is a string Spark will not validate: a typo produces a
silently ignored setting, or a ClassNotFoundException at first use. Asserting
them here is cheaper than finding out on a cluster.
"""

from __future__ import annotations

import pytest

from pipeline.session import PACKAGES, CatalogConfig, catalog_options

REST = CatalogConfig(
    name="lake",
    type="rest",
    warehouse="s3://warehouse/",
    uri="http://iceberg-rest:8181",
    s3_endpoint="http://minio:9000",
)
LOCAL = CatalogConfig(name="lake", type="hadoop", warehouse="/tmp/warehouse")


def test_rest_catalog_options() -> None:
    assert catalog_options(REST) == {
        "spark.jars.packages": PACKAGES,
        "spark.sql.extensions": (
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        ),
        "spark.sql.catalog.lake": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.lake.type": "rest",
        "spark.sql.catalog.lake.warehouse": "s3://warehouse/",
        "spark.sql.catalog.lake.uri": "http://iceberg-rest:8181",
        "spark.sql.catalog.lake.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.catalog.lake.s3.endpoint": "http://minio:9000",
        "spark.sql.catalog.lake.s3.path-style-access": "true",
        "spark.sql.catalog.lake.client.region": "us-east-1",
    }


def test_local_catalog_has_no_s3_options() -> None:
    """A directory-backed catalog must not carry S3FileIO, which would fail."""
    options = catalog_options(LOCAL)
    assert options["spark.sql.catalog.lake.type"] == "hadoop"
    assert not [k for k in options if ".s3." in k or k.endswith(".io-impl")]


def test_packages_pin_one_iceberg_version() -> None:
    """A runtime and an AWS bundle on different versions fail at first S3 call."""
    versions = {p.rsplit(":", 1)[1] for p in PACKAGES.split(",")}
    assert len(versions) == 1
    assert "iceberg-spark-runtime-4.0_2.13" in PACKAGES
    assert "iceberg-aws-bundle" in PACKAGES


def test_catalog_name_is_used_everywhere() -> None:
    """A renamed catalog must not leave keys pointing at the old name."""
    options = catalog_options(CatalogConfig(name="other", type="hadoop", warehouse="/w"))
    scoped = [k for k in options if k.startswith("spark.sql.catalog.")]
    assert scoped and all(k.split(".")[3] == "other" for k in scoped)


def test_from_env_returns_none_when_unconfigured() -> None:
    """The gate: no warehouse configured means a plain offline session."""
    assert CatalogConfig.from_env({}) is None
    assert CatalogConfig.from_env({"ICEBERG_URI": "http://x:8181"}) is None


def test_from_env_reads_the_compose_settings() -> None:
    cfg = CatalogConfig.from_env(
        {
            "ICEBERG_WAREHOUSE": "s3://warehouse/",
            "ICEBERG_URI": "http://iceberg-rest:8181",
            "S3_ENDPOINT": "http://minio:9000",
        }
    )
    assert cfg == REST


@pytest.mark.parametrize("key", ["ICEBERG_CATALOG", "ICEBERG_CATALOG_TYPE", "AWS_REGION"])
def test_from_env_default(key: str) -> None:
    cfg = CatalogConfig.from_env({"ICEBERG_WAREHOUSE": "/w"})
    assert cfg is not None
    assert {
        "ICEBERG_CATALOG": cfg.name,
        "ICEBERG_CATALOG_TYPE": cfg.type,
        "AWS_REGION": cfg.region,
    }[key] in ("lake", "rest", "us-east-1")
