"""Entry point. parse a mode and delegate."""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence

from pipeline.runner import batch
from pipeline.session import CatalogConfig, build_session

log = logging.getLogger("pipeline")

MODES = ("report", "batch")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="pipeline")
    parser.add_argument("--mode", choices=MODES, default="report")
    return parser.parse_args(argv)


def report(spark: object, catalog: CatalogConfig | None) -> None:
    log.info("spark         %s", getattr(spark, "version", "?"))
    log.info("python        %s", sys.version.split()[0])
    log.info("catalog       %s", f"{catalog.name} ({catalog.type})" if catalog else "none")


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    args = parse_args(argv)

    catalog = CatalogConfig.from_env()
    if args.mode == "batch" and catalog is None:
        log.error("batch mode needs a catalog; set ICEBERG_WAREHOUSE")
        return 1

    spark = build_session(catalog=catalog)
    spark.sparkContext.setLogLevel("WARN")
    try:
        log.info("master   %s", spark.sparkContext.master)
        if args.mode == "batch":
            assert catalog is not None
            batch.run(spark, catalog.name)
        else:
            report(spark, catalog)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
