"""Building the SparkSession, including the Iceberg catalog wiring.

Split out of main.py because the runners need it too, and because catalog
configuration is the part that differs most between a laptop, CI and a cluster.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

from pyspark.sql import SparkSession

ICEBERG_VERSION = "1.11.0"
PACKAGES = (
    f"org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:{ICEBERG_VERSION},"
    f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}"
)
EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"


@dataclass(frozen=True)
class CatalogConfig:
    """Where the tables live.

    `rest` is the Compose stack: a REST catalog in front of object storage.
    `hadoop` is a plain directory, which needs no service and is how the local
    experiments in the walkthrough run.
    """

    name: str
    type: str
    warehouse: str
    uri: str | None = None
    s3_endpoint: str | None = None
    region: str = "us-east-1"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> CatalogConfig | None:
        """None when nothing is configured, so a bare `make run` stays offline."""
        e = os.environ if env is None else env
        warehouse = e.get("ICEBERG_WAREHOUSE")
        if not warehouse:
            return None
        return cls(
            name=e.get("ICEBERG_CATALOG", "lake"),
            type=e.get("ICEBERG_CATALOG_TYPE", "rest"),
            warehouse=warehouse,
            uri=e.get("ICEBERG_URI"),
            s3_endpoint=e.get("S3_ENDPOINT"),
            region=e.get("AWS_REGION", "us-east-1"),
        )


def catalog_options(cfg: CatalogConfig) -> dict[str, str]:
    """The Spark configuration for one Iceberg catalog."""
    at = f"spark.sql.catalog.{cfg.name}"
    options = {
        "spark.jars.packages": PACKAGES,
        "spark.sql.extensions": EXTENSIONS,
        at: "org.apache.iceberg.spark.SparkCatalog",
        f"{at}.type": cfg.type,
        f"{at}.warehouse": cfg.warehouse,
    }
    if cfg.uri:
        options[f"{at}.uri"] = cfg.uri
    if cfg.s3_endpoint:
        # path-style because a bucket-as-subdomain needs DNS the local stack
        # does not have.
        options[f"{at}.io-impl"] = "org.apache.iceberg.aws.s3.S3FileIO"
        options[f"{at}.s3.endpoint"] = cfg.s3_endpoint
        options[f"{at}.s3.path-style-access"] = "true"
        options[f"{at}.client.region"] = cfg.region
    return options


def launcher_set_master() -> bool:
    """True when spark-submit has already chosed the master.

    spark-submit starts the JVM first and hands the Python process its py4j
    gateway port. SparkConf cannot be asked instead: with no JVM yet it falls
    back to an empty dict, so it reports no master even under spark-submit.
    """
    return "PYSPARK_GATEWAY_PORT" in os.environ


def build_session(app_name: str = "pipeline", catalog: CatalogConfig | None = None) -> SparkSession:
    """Build a session, defaulting to local[*] only when nothing else decides."""
    builder = SparkSession.builder
    if not launcher_set_master():
        builder = builder.appName(app_name).master("local[*]")
    if catalog is not None:
        for key, value in catalog_options(catalog).items():
            builder = builder.config(key, value)
    return builder.getOrCreate()
