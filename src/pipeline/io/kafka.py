"""Reading Kafka topics that carry Confluent-framed Avro.

Thin, like the Iceberg adapter: decode, name the columns, attach a watermark,
and hand a DataFrame to the core.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

from pipeline.streams import StreamConfig, load_schema

# Magic byte plus a four-byte big-endian schema id.
HEADER_BYTES = 5


def decode(frame: DataFrame, schema: Mapping[str, Any]) -> DataFrame:
    """Strip the Confluent framing, then read the Avro body.

    from_avro takes a schema string and knows nothing about a registry, so the
    prefix the producer wrote has to come off by hand. substring is 1-indexed.
    """
    body = F.expr(f"substring(value, {HEADER_BYTES + 1}, length(value) - {HEADER_BYTES})")
    return frame.select(from_avro(body, json.dumps(dict(schema))).alias("record")).select(
        "record.*"
    )


def read_stream(
    spark: SparkSession,
    cfg: StreamConfig,
    topic: str,
    schema: str,
    event_time: str,
    lateness: str,
) -> DataFrame:
    """One watermarked stream.

    The watermark is attached here rather than in the core, because how late
    an event may be is a property of the source, not of the join.
    """
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.bootstrap_servers)
        .option("subscribe", topic)
        # earliest so a restart replays the demo rather than skipping it. A
        # real deployment reads from the checkpoint and this is ignored.
        .option("startingOffsets", "earliest")
        .load()
    )
    # Every streaming source reports its columns nullable, whatever schema it
    # was given, so the declared contract has to be enforced rather than
    # assumed. None of these events has an optional field, so any null means a
    # record that does not match its own schema.
    decoded = decode(raw, load_schema(schema)).dropna()
    return decoded.withWatermark(event_time, lateness)
