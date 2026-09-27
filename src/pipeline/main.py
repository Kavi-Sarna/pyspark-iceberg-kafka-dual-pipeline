"""Entry point.

Builds a SparkSession, reports what it is running on, and stops cleanly.
"""

from __future__ import annotations

import logging
import os
import sys

from pyspark.sql import SparkSession

log = logging.getLogger("pipeline")


def launcher_set_master() -> bool:
    """True when spark-submit has already chosen the master.

    spark-submit starts the JVM first and hands the Python process its py4j
    gateway port. SparkConf cannot be asked instead: with no JVM yet it falls
    back to an empty dict, so it reports no master even under spark-submit.
    """
    return "PYSPARK_GATEWAY_PORT" in os.environ


def build_session(app_name: str = "pipeline") -> SparkSession:
    """Build a session, defaulting to local[*] only when nothing else decides."""
    builder = SparkSession.builder
    if not launcher_set_master():
        builder = builder.appName(app_name).master("local[*]")
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
