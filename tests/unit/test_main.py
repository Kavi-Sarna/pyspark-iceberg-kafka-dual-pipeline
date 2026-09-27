"""The toolchain, not the pipeline: a session builds, reports itself, and stops."""

from __future__ import annotations

from pipeline.main import build_session


def test_session_builds_and_reports_spark_4() -> None:
    spark = build_session("pipeline-test")
    try:
        assert spark.version.startswith("4.0")
    finally:
        spark.stop()


def test_main_exits_zero() -> None:
    from pipeline.main import main

    assert main() == 0
