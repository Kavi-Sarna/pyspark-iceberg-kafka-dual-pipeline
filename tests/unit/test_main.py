"""The toolchain, not the pipeline: a session builds, reports itself, and stops."""

from __future__ import annotations

import pytest

from pipeline.main import build_session, launcher_set_master


def test_session_builds_and_reports_spark_4() -> None:
    spark = build_session("pipeline-test")
    try:
        assert spark.version.startswith("4.0")
    finally:
        spark.stop()


def test_main_exits_zero() -> None:
    from pipeline.main import main

    assert main() == 0


@pytest.mark.parametrize(
    ("env", "expected"),
    [({}, False), ({"PYSPARK_GATEWAY_PORT": "35123"}, True)],
)
def test_launcher_set_master(
    monkeypatch: pytest.MonkeyPatch, env: dict[str, str], expected: bool
) -> None:
    """Under spark-submit the master is already chosen and must not be overridden.

    SparkConf cannot answer this: with no JVM yet it returns an empty conf, so
    it reports no master even under spark-submit, and the session would silently
    fall back to local[*] on a cluster job.
    """
    monkeypatch.delenv("PYSPARK_GATEWAY_PORT", raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    assert launcher_set_master() is expected


def test_local_session_is_local(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PYSPARK_GATEWAY_PORT", raising=False)
    spark = build_session("pipeline-local-test")
    try:
        assert spark.sparkContext.master == "local[*]"
    finally:
        spark.stop()
