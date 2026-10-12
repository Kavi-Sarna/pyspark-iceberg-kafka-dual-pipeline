"""Configuration, layered: packaged file, override file, environment.

Every value here is a string Spark or Kafka will not validate, so the point of
these tests is that a wrong one is loud rather than silent.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

from pipeline.config import AppConfig, parse_duration
from pipeline.session import catalog_options

LOCAL = {"ICEBERG_WAREHOUSE": "/tmp/w", "ICEBERG_CATALOG_TYPE": "hadoop"}
S3 = {"ICEBERG_WAREHOUSE": "s3://warehouse/"}


def test_the_packaged_defaults_load() -> None:
    """The file ships inside the package, so the image carries it."""
    app = AppConfig.load(env={})
    assert app.tables.namespace == "ops"
    assert app.tables.output == "shipment_deliveries"
    assert app.stream.lateness == "10 minutes"
    assert app.stream.max_lag == timedelta(minutes=30)


def test_nothing_is_configured_without_a_switch() -> None:
    """A bare run reaches no network. warehouse and bootstrap_servers are
    deliberately absent from the packaged file for exactly this reason."""
    app = AppConfig.load(env={})
    assert app.catalog is None
    assert app.kafka is None


def test_a_warehouse_turns_the_catalog_on() -> None:
    assert AppConfig.load(env=LOCAL).catalog is not None
    assert AppConfig.load(env={"KAFKA_BOOTSTRAP_SERVERS": "k:9092"}).kafka is not None


def test_credentials_come_only_from_the_environment() -> None:
    """The TOML is committed. Nothing in it may be a secret."""
    plain = AppConfig.load(env=S3).catalog
    assert plain is not None
    assert plain.access_key is None

    with_creds = AppConfig.load(env={**S3, "AWS_ACCESS_KEY_ID": "k", "AWS_SECRET_ACCESS_KEY": "s"})
    assert with_creds.catalog is not None
    assert with_creds.catalog.access_key == "k"


def test_a_directory_warehouse_gets_no_s3_options() -> None:
    """The file's s3_endpoint default must not reach a local catalog, which
    would try to contact MinIO to read a path on disk."""
    catalog = AppConfig.load(env=LOCAL).catalog
    assert catalog is not None
    assert catalog.s3_endpoint is None
    assert not [k for k in catalog_options(catalog) if ".s3." in k]


def test_an_object_store_warehouse_does() -> None:
    catalog = AppConfig.load(env=S3).catalog
    assert catalog is not None
    assert catalog.s3_endpoint == "http://minio:9000"


def test_an_override_file_wins_over_the_packaged_one(tmp_path: Path) -> None:
    override = tmp_path / "app.toml"
    override.write_text('[stream]\nlateness = "2 minutes"\nmax_lag = "5 minutes"\n')

    app = AppConfig.load(override, env={})

    assert app.stream.lateness == "2 minutes"
    assert app.stream.max_lag == timedelta(minutes=5)
    # Untouched sections survive the merge rather than being replaced.
    assert app.tables.namespace == "ops"
    assert app.stream.trigger == "15 seconds"


def test_the_environment_wins_over_both(tmp_path: Path) -> None:
    override = tmp_path / "app.toml"
    override.write_text('[catalog]\nname = "from_file"\n')

    app = AppConfig.load(override, env={**LOCAL, "ICEBERG_CATALOG": "from_env"})

    assert app.catalog is not None
    assert app.catalog.name == "from_env"


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("10 minutes", timedelta(minutes=10)),
        ("1 hour", timedelta(hours=1)),
        ("30 seconds", timedelta(seconds=30)),
        ("  2 days  ", timedelta(days=2)),
    ],
)
def test_durations_parse(text: str, expected: timedelta) -> None:
    """One spelling serves both Spark's watermark string and JoinSpec's
    timedelta. Writing the number twice is how the two drift apart."""
    assert parse_duration(text) == expected


@pytest.mark.parametrize("text", ["soon", "10", "minutes", "10 fortnights", ""])
def test_a_bad_duration_fails_loudly(text: str) -> None:
    with pytest.raises(ValueError, match="duration"):
        parse_duration(text)


def test_event_logging_is_off_until_a_directory_exists() -> None:
    """Spark refuses to start if the event log directory is missing, so the
    packaged default cannot be on: a bare run would depend on a path nobody
    created. The Compose stack turns it on, where the volume is."""
    assert not AppConfig.load(env={}).events.enabled
    assert AppConfig.load(env={"EVENT_LOG_ENABLED": "true"}).events.enabled


def test_rolling_is_set_explicitly_even_though_spark_4_defaults_to_it() -> None:
    """Spark 4 rolls by default; saying so keeps the file honest about what
    the job does rather than relying on a default that moved once already."""
    options = AppConfig.load(env={"EVENT_LOG_ENABLED": "true"}).events.spark_options()
    assert options["spark.eventLog.rolling.enabled"] == "true"
    assert options["spark.eventLog.rolling.maxFileSize"] == "128m"


def test_retention_is_bounded() -> None:
    """The setting that actually limits disk.

    Spark's default for maxFilesToRetain is Int.MaxValue, so every part file
    is kept forever and rolling alone bounds nothing. A streaming query runs
    for weeks.
    """
    history = AppConfig.load(env={}).events.history_options()
    assert int(history["spark.history.fs.eventLog.rolling.maxFilesToRetain"]) == 20


def test_the_history_server_reads_where_the_job_writes() -> None:
    """One value, two settings. Writing the path twice is how they diverge
    and the History Server quietly shows nothing."""
    events = AppConfig.load(env={"EVENT_LOG_ENABLED": "true"}).events
    assert (
        events.spark_options()["spark.eventLog.dir"]
        == (events.history_options()["spark.history.fs.logDirectory"])
    )


def test_the_event_log_directory_is_overridable() -> None:
    events = AppConfig.load(env={"EVENT_LOG_DIR": "s3a://warehouse/spark-events"}).events
    assert events.dir == "s3a://warehouse/spark-events"
    assert events.history_options()["spark.history.fs.logDirectory"] == events.dir


def test_disabling_it_says_so_rather_than_omitting_it() -> None:
    """An absent spark.eventLog.enabled means 'whatever the cluster default
    is'. False means false."""
    import dataclasses

    events = dataclasses.replace(AppConfig.load(env={}).events, enabled=False)
    assert events.spark_options() == {"spark.eventLog.enabled": "false"}


def test_metrics_are_off_until_a_collector_is_named() -> None:
    """Same reasoning as the event log. A bare run should not spray UDP at a
    hostname that only resolves inside Compose."""
    assert AppConfig.load(env={}).metrics.enabled is False


def test_metrics_enabled_is_a_flag_not_a_string() -> None:
    assert AppConfig.load(env={"METRICS_ENABLED": "true"}).metrics.enabled is True
    assert AppConfig.load(env={"METRICS_ENABLED": "false"}).metrics.enabled is False


def test_the_collector_address_is_overridable() -> None:
    metrics = AppConfig.load(env={"STATSD_HOST": "10.0.0.5", "STATSD_PORT": "8125"}).metrics
    assert (metrics.host, metrics.port) == ("10.0.0.5", 8125)


def test_the_prefix_is_not_overridable_by_environment() -> None:
    """It is half of a metric name. Changing it per deployment means the same
    query appears as two different series and no dashboard matches both."""
    assert AppConfig.load(env={"STATSD_PREFIX": "other"}).metrics.prefix == "pipeline.stream"
