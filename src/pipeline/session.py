"""Building the SparkSession, including the Iceberg catalog wiring.

Split out of main.py because the runners need it too, and because catalog
configuration is the part that differs most between a laptop, CI and a cluster.
"""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from pyspark.sql import SparkSession

ICEBERG_VERSION = "1.11.0"
PACKAGES = (
    f"org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:{ICEBERG_VERSION},"
    f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}"
)
EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"

SPARK_VERSION = "4.0.4"
# spark-avro is not in the pyspark wheel: only the Avro library is, not the
# connector that provides from_avro.
STREAM_PACKAGES = (
    f"org.apache.spark:spark-sql-kafka-0-10_2.13:{SPARK_VERSION},"
    f"org.apache.spark:spark-avro_2.13:{SPARK_VERSION}"
)


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
    access_key: str | None = None
    secret_key: str | None = None


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
        # Given to S3FileIO directly rather than left to the AWS SDK's default
        # credential chain. The chain reads the environment of whichever JVM
        # ends up doing the I/O, and under spark-submit that is not reliably
        # the process holding these variables.
        if cfg.access_key and cfg.secret_key:
            options[f"{at}.s3.access-key-id"] = cfg.access_key
            options[f"{at}.s3.secret-access-key"] = cfg.secret_key
    return options


def launcher_set_master() -> bool:
    """True when spark-submit has already chosed the master.

    spark-submit starts the JVM first and hands the Python process its py4j
    gateway port. SparkConf cannot be asked instead: with no JVM yet it falls
    back to an empty dict, so it reports no master even under spark-submit.
    """
    return "PYSPARK_GATEWAY_PORT" in os.environ


def build_session(
    app_name: str = "pipeline",
    catalog: CatalogConfig | None = None,
    extra_packages: Sequence[str] = (),
    event_options: Mapping[str, str] | None = None,
) -> SparkSession:
    """Build a session, defaulting to local[*] only when nothing else decides."""
    builder = SparkSession.builder
    if not launcher_set_master():
        builder = builder.appName(app_name).master("local[*]")

    options = dict(catalog_options(catalog)) if catalog is not None else {}
    if event_options:
        options.update(event_options)
    # One comma-separated string, so the streaming jars have to be merged with
    # the catalog's rather than set alongside them.
    packages = [p for p in (options.get("spark.jars.packages"), *extra_packages) if p]
    if packages:
        options["spark.jars.packages"] = ",".join(packages)

    for key, value in options.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
