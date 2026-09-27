"""Entry point.

Builds a SparkSession, reports what it is running on, and stops cleanly.
"""

from __future__ import annotations

import logging
import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession

log = logging.getLogger("pipeline")


def build_session(app_name: str = "pipeline") -> SparkSession:
    """Build a session, defaulting to local[*] only when no master is configured.

    spark-submit sets spark.master in the conf; overriding it here would ignore
    whatever cluster the job was submitted to.
    """
    builder = SparkSession.builder.appName(app_name)
    if not SparkConf().contains("spark.master"):
        builder = builder.master("local[*]")
    return builder.getOrCreate()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        log.info("spark    %s", spark.version)
        log.info("python   %s", sys.version.split()[0])
        log.info("master   %s", spark.sparkContext.master)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
