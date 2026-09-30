"""Entry point.

Builds a SparkSession, reports what it is running on, and stops cleanly.
"""

from __future__ import annotations

import logging
import sys

from pipeline.session import CatalogConfig, build_session

log = logging.getLogger("pipeline")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    catalog = CatalogConfig.from_env()
    spark = build_session(catalog=catalog)
    spark.sparkContext.setLogLevel("WARN")
    try:
        log.info("spark    %s", spark.version)
        log.info("python   %s", sys.version.split()[0])
        log.info("master   %s", spark.sparkContext.master)
        log.info("catalog  %s", f"{catalog.name} ({catalog.type})" if catalog else "none")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
