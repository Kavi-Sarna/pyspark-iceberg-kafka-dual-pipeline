"""All configuration, read once, into frozen dataclasses.

Three layers, each overriding the one before:

  1. the packaged conf/app.toml, which ships inside the image
  2. a file named by --config or PIPELINE_CONFIG, if given
  3. environment variables, for per-deployment values and for secrets

Credentials come only from the environment. The TOML is committed, so it
describes shape and never holds a secret.
"""

from __future__ import annotations

import os
import re
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from importlib import resources
from pathlib import Path
from typing import Any

from pipeline.session import CatalogConfig
from pipeline.streams import StreamConfig

CONFIG_ENV = "PIPELINE_CONFIG"

_DURATION = re.compile(r"^\s*(\d+)\s*(seconds?|minutes?|hours?|days?)\s*$")


def parse_duration(text: str) -> timedelta:
    """Accept the same spelling Spark uses for watermarks, so one value serves both.

    Spark wants "10 minutes" as a string; JoinSpec wants a timedelta. Writing
    the number twice is how the two drift apart.
    """
    match = _DURATION.match(text)
    if not match:
        raise ValueError(f"cannot read {text!r} as a duration, e.g. '10 minutes'")
    amount, unit = int(match.group(1)), match.group(2).rstrip("s")
    return timedelta(**{f"{unit}s": amount})


@dataclass(frozen=True)
class Tables:
    namespace: str
    shipments: str
    deliveries: str
    output: str


@dataclass(frozen=True)
class StreamSettings:
    """Tuning that used to be module constants.

    Changing how long state is kept should not be a code change.
    """

    lateness: str
    max_lag: timedelta
    trigger: str
    checkpoint: str


@dataclass(frozen=True)
class AppConfig:
    tables: Tables
    stream: StreamSettings
    catalog: CatalogConfig | None
    kafka: StreamConfig | None

    @classmethod
    def load(
        cls, path: str | Path | None = None, env: Mapping[str, str] | None = None
    ) -> AppConfig:
        e = os.environ if env is None else env
        data = _packaged()
        override = path or e.get(CONFIG_ENV)
        if override:
            _merge(data, tomllib.loads(Path(override).read_text()))

        tables = Tables(**data["tables"])
        raw_stream = dict(data["stream"])
        stream = StreamSettings(
            lateness=raw_stream["lateness"],
            max_lag=parse_duration(raw_stream["max_lag"]),
            trigger=raw_stream["trigger"],
            checkpoint=e.get("CHECKPOINT_LOCATION", raw_stream["checkpoint"]),
        )
        return cls(
            tables=tables,
            stream=stream,
            catalog=_catalog(data["catalog"], e),
            kafka=_kafka(data["kafka"], e),
        )


def _packaged() -> dict[str, Any]:
    text = resources.files("pipeline").joinpath("conf/app.toml").read_text()
    return tomllib.loads(text)


def _merge(base: dict[str, Any], extra: Mapping[str, Any]) -> None:
    """One level deep, which is all this file has."""
    for section, values in extra.items():
        if isinstance(values, dict) and isinstance(base.get(section), dict):
            base[section].update(values)
        else:
            base[section] = values


def _catalog(section: Mapping[str, Any], e: Mapping[str, str]) -> CatalogConfig | None:
    """None when no warehouse is configured, so a bare run stays offline."""
    warehouse = e.get("ICEBERG_WAREHOUSE", section.get("warehouse"))
    if not warehouse:
        return None
    # Only object-store warehouses get S3FileIO. A directory warehouse that
    # inherited the file's s3_endpoint would try to reach MinIO to read a
    # local path.
    s3_endpoint = None
    if warehouse.startswith("s3"):
        s3_endpoint = e.get("S3_ENDPOINT", section.get("s3_endpoint"))

    return CatalogConfig(
        name=e.get("ICEBERG_CATALOG", section["name"]),
        type=e.get("ICEBERG_CATALOG_TYPE", section["type"]),
        warehouse=warehouse,
        uri=e.get("ICEBERG_URI", section.get("uri")),
        s3_endpoint=s3_endpoint,
        region=e.get("AWS_REGION", section.get("region", "us-east-1")),
        # Environment only. Never read from the file.
        access_key=e.get("AWS_ACCESS_KEY_ID"),
        secret_key=e.get("AWS_SECRET_ACCESS_KEY"),
    )


def _kafka(section: Mapping[str, Any], e: Mapping[str, str]) -> StreamConfig | None:
    servers = e.get("KAFKA_BOOTSTRAP_SERVERS", section.get("bootstrap_servers"))
    if not servers:
        return None
    return StreamConfig(
        bootstrap_servers=servers,
        schema_registry=e.get("SCHEMA_REGISTRY_URL", section["schema_registry"]),
    )
