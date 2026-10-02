"""The parts of the batch path that need no Spark."""

from __future__ import annotations

import pytest

from pipeline.core.schemas import JOIN_KEY
from pipeline.io.iceberg import table_name
from pipeline.main import parse_args
from pipeline.runner import batch


def test_table_name_is_three_parts() -> None:
    assert table_name("lake", "ops", "shipments") == "lake.ops.shipments"


def test_batch_spec_joins_on_the_declared_key() -> None:
    assert batch.SPEC.keys == (JOIN_KEY,)


def test_batch_spec_has_no_time_bound() -> None:
    """Batch does not need one. Streaming will, and supplies its own."""
    assert batch.SPEC.max_lag is None


def test_mode_defaults_to_report() -> None:
    """A bare run must not try to toucha catalog."""
    assert parse_args([]).mode == "report"


def test_mode_accepts_batch() -> None:
    assert parse_args(["--mode", "batch"]).mode == "batch"


def test_unknown_mode_is_rejected() -> None:
    with pytest.raises(SystemExit):
        parse_args(["--mode", "sideways"])
