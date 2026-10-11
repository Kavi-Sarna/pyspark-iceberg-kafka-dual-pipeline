"""Operational drills: the things you want to have done before you have to.

Each one performs a real operation against a real query or a real table and
asserts what happened. They are tests rather than a script so that the
answers cannot quietly stop being true, and `docs/drills.md` records them in
prose with the numbers these produce.

File sources rather than Kafka, and a directory catalog rather than MinIO:
the behaviour being exercised belongs to Spark and to Iceberg, and a broker
would only make the drills slower and less deterministic.
"""

from __future__ import annotations

import json
import shutil
import time
from collections.abc import Iterator
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from py4j.protocol import Py4JJavaError
from pyspark.errors import IllegalArgumentException, StreamingQueryException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import to_avro

from pipeline.config import AppConfig
from pipeline.core.join import left_join
from pipeline.core.schemas import DELIVERY, SHIPMENT
from pipeline.io.kafka import decode
from pipeline.metrics import metrics_from
from pipeline.runner import stream

BASE = datetime(2026, 9, 13, 10, 0)
SETTINGS = AppConfig.load(env={}).stream
SPEC = stream.spec_for(SETTINGS)


def at(minutes: int) -> datetime:
    return BASE + timedelta(minutes=minutes)


class Drill:
    """Two file sources, one checkpoint, an Iceberg sink.

    The sink matters. A parquet sink keeps its own `_spark_metadata` commit
    log and skips a batch it has already written, which hides the duplication
    drill entirely. Iceberg has no such log, and Iceberg is what the job
    actually writes to.
    """

    def __init__(self, spark: SparkSession, root: Path, table: str) -> None:
        self.spark = spark
        self.root = root
        self.table = table
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.drills")
        # Dropped first rather than IF NOT EXISTS: these drills count rows, so
        # resuming on top of a previous run's leftovers would produce a
        # convincing "it duplicated" for entirely the wrong reason.
        spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
        spark.sql(f"""CREATE TABLE {table} (
            shipment_id STRING, carrier STRING, dispatch_time TIMESTAMP,
            delivery_id STRING, delivered_time TIMESTAMP) USING iceberg""")
        self.shipments()
        self.deliveries()

    def shipments(self, *rows: tuple[str, str, datetime]) -> None:
        self.spark.createDataFrame(list(rows), SHIPMENT).write.mode("append").parquet(
            str(self.root / "ship")
        )

    def deliveries(self, *rows: tuple[str, str, datetime]) -> None:
        self.spark.createDataFrame(list(rows), DELIVERY).write.mode("append").parquet(
            str(self.root / "dlv")
        )

    def sources(self) -> tuple[DataFrame, DataFrame]:
        left = (
            self.spark.readStream.schema(SHIPMENT)
            .parquet(str(self.root / "ship"))
            .dropna()
            .withWatermark("dispatch_time", SETTINGS.lateness)
        )
        right = (
            self.spark.readStream.schema(DELIVERY)
            .parquet(str(self.root / "dlv"))
            .dropna()
            .withWatermark("delivered_time", SETTINGS.lateness)
        )
        return left, right

    def run(self) -> None:
        """Process what is available, then stop. availableNow bounds it."""
        left, right = self.sources()
        query = (
            left_join(left, right, SPEC)
            .writeStream.outputMode("append")
            .option("checkpointLocation", str(self.root / "ck"))
            .trigger(availableNow=True)
            .toTable(self.table)
        )
        query.awaitTermination(300)

    def run_a_different_query(self) -> None:
        """One source instead of two: a topology change, against the same
        checkpoint."""
        left, _ = self.sources()
        query = (
            left.dropDuplicates(["shipment_id"])
            .selectExpr(
                "shipment_id",
                "carrier",
                "dispatch_time",
                "CAST(NULL AS STRING) AS delivery_id",
                "CAST(NULL AS TIMESTAMP) AS delivered_time",
            )
            .writeStream.outputMode("append")
            .option("checkpointLocation", str(self.root / "ck"))
            .trigger(availableNow=True)
            .toTable(self.table)
        )
        query.awaitTermination(300)

    def drop_checkpoint(self) -> None:
        shutil.rmtree(self.root / "ck")

    def rows(self) -> int:
        return int(self.spark.table(self.table).count())


@pytest.fixture
def drill(iceberg: SparkSession, tmp_path: Path, request: pytest.FixtureRequest) -> Iterator[Drill]:
    # The hash rather than the name alone: truncating a long test name to fit
    # an identifier is how two tests come to share a table.
    table = f"lake.drills.d_{abs(hash(request.node.name)) % 10**8}"
    harness = Drill(iceberg, tmp_path, table)
    harness.shipments(("shp-1", "carrier-a", at(0)), ("shp-2", "carrier-b", at(5)))
    harness.deliveries(("dlv-1", "shp-1", at(2)))
    yield harness
    iceberg.sql(f"DROP TABLE IF EXISTS {table} PURGE")


# --- Drill 1: kill the driver, restart -------------------------------------


def test_a_restart_resumes_from_the_checkpoint(drill: Drill) -> None:
    """The drill: stop a running query and start it again unchanged.

    Nothing is reprocessed. The checkpoint holds the offsets already read and
    the state already built, so the second run begins where the first stopped
    rather than at the beginning of the topic."""
    drill.run()
    after_first = drill.rows()
    assert after_first > 0

    drill.run()

    assert drill.rows() == after_first


# --- Drill 2: delete the checkpoint ----------------------------------------


def test_deleting_the_checkpoint_duplicates_the_output(drill: Drill) -> None:
    """The drill: delete the checkpoint and restart, with the output intact.

    Every row is read again and written again. The sink has no memory of what
    it already holds, so "start over" means "emit it all twice". This is the
    cost of treating a checkpoint as scratch space."""
    drill.run()
    before = drill.rows()

    drill.drop_checkpoint()
    drill.run()

    assert drill.rows() == before * 2


# --- Drill 3: change the query ---------------------------------------------


def test_changing_the_query_is_refused_against_an_old_checkpoint(drill: Drill) -> None:
    """The drill: change the query's shape and restart on the same checkpoint.

    Spark refuses rather than guessing. The offset log records which sources
    the query had; a query with a different set of them cannot be the same
    query, and resuming would mean inventing offsets for a source that was
    never read."""
    drill.run()

    with pytest.raises(StreamingQueryException) as caught:
        drill.run_a_different_query()

    message = str(caught.value)
    assert "There are [2] sources in the checkpoint offsets" in message
    assert "now there are [1] sources requested by the query" in message
    assert "Cannot continue" in message


# --- Drill 4: evolve the Avro schema ---------------------------------------

V1: dict[str, Any] = {
    "type": "record",
    "name": "Shipment",
    "namespace": "ops",
    "fields": [
        {"name": "shipment_id", "type": "string"},
        {"name": "carrier", "type": "string"},
    ],
}
V2_OPTIONAL: dict[str, Any] = {
    **V1,
    "fields": [
        *V1["fields"],
        {"name": "service_level", "type": ["null", "string"], "default": None},
    ],
}
V2_REQUIRED: dict[str, Any] = {
    **V1,
    "fields": [*V1["fields"], {"name": "service_level", "type": "string"}],
}
# The same field, inserted rather than appended. Every byte after it moves.
V2_INSERTED: dict[str, Any] = {
    **V1,
    "fields": [
        V1["fields"][0],
        {"name": "service_level", "type": "string"},
        V1["fields"][1],
    ],
}


def encoded(spark: SparkSession, schema: dict[str, Any], *values: str) -> DataFrame:
    """A Confluent-framed record, the way the producer writes one.

    The five-byte prefix is part of the drill, not scaffolding: discarding it
    is what leaves the decoder with no writer schema, which is the whole
    subject of drill 4.
    """
    names = [f["name"] for f in schema["fields"]]
    frame = spark.createDataFrame([tuple(values)], names)
    header = F.lit(bytearray(b"\x00\x00\x00\x00\x01"))  # magic byte, schema id 1
    return frame.select(
        F.concat(header, to_avro(F.struct(*names), json.dumps(schema))).alias("value")
    )


def decoded(frame: DataFrame, schema: dict[str, Any]) -> list[dict[str, Any]]:
    """The production decoder, not a copy of it.

    Calling io.kafka.decode means HEADER_BYTES and the substring are exercised
    here too, so the conclusions below stay tied to the code that ships.
    """
    return [row.asDict() for row in decode(frame, schema).collect()]


def test_a_writer_may_add_a_field_an_old_reader_ignores(iceberg: SparkSession) -> None:
    """The drill: add a field to the producer and leave the consumer behind.

    Safe. Avro's binary encoding is positional, so a reader that wants two
    fields reads two fields and never looks at the bytes that follow."""
    body = encoded(iceberg, V2_OPTIONAL, "shp-1", "carrier-a", "express")

    assert decoded(body, V1) == [{"shipment_id": "shp-1", "carrier": "carrier-a"}]


def test_even_a_required_field_is_invisible_to_an_old_reader(iceberg: SparkSession) -> None:
    """The same, with a field that has no default. "Required" constrains the
    writer; it says nothing to a reader that stops before reaching it."""
    body = encoded(iceberg, V2_REQUIRED, "shp-1", "carrier-a", "express")

    assert decoded(body, V1) == [{"shipment_id": "shp-1", "carrier": "carrier-a"}]


def test_a_reader_ahead_of_the_writer_fails_even_on_an_optional_field(
    iceberg: SparkSession,
) -> None:
    """The drill that matters: deploy the consumer first.

    It fails, and the optional-with-a-default makes no difference. A default
    is used during *schema resolution*, which needs the writer's schema - and
    io.kafka.decode throws the writer's schema id away with the Confluent
    header and hands from_avro a fixed schema. There is no resolution here to
    apply a default during. The reader runs off the end of the record."""
    body = encoded(iceberg, V1, "shp-1", "carrier-a")

    with pytest.raises(Py4JJavaError) as caught:
        decoded(body, V2_OPTIONAL)

    # The reader runs off the end of the record, and Spark reports it the only
    # way it can: these bytes do not match the schema it was handed.
    assert "Malformed records are detected in record parsing" in str(caught.value)


# --- Drill 5: Iceberg maintenance ------------------------------------------


@pytest.fixture
def maintenance(iceberg: SparkSession, request: pytest.FixtureRequest) -> Iterator[str]:
    table = f"lake.drills.m_{abs(hash(request.node.name)) % 10**8}"
    iceberg.sql("CREATE NAMESPACE IF NOT EXISTS lake.drills")
    iceberg.sql(f"CREATE TABLE {table} (id BIGINT, v STRING) USING iceberg")
    for i in range(6):
        iceberg.sql(f"INSERT INTO {table} VALUES ({i}, 'v{i}')")
    yield table
    iceberg.sql(f"DROP TABLE IF EXISTS {table} PURGE")


def count_of(spark: SparkSession, table: str, metadata: str) -> int:
    return int(spark.sql(f"SELECT count(*) c FROM {table}.{metadata}").collect()[0].c)


def test_compaction_merges_files_without_touching_the_data(
    iceberg: SparkSession, maintenance: str
) -> None:
    """The drill: six single-row commits, then rewrite_data_files.

    Six files become one and the rows are unchanged. Each commit wrote its
    own file, which is how a streaming sink behaves at every trigger, and is
    the problem compaction exists for."""
    assert count_of(iceberg, maintenance, "files") == 6

    result = iceberg.sql(
        f"CALL lake.system.rewrite_data_files("
        f"table => '{maintenance.split('.', 1)[1]}', options => map('min-input-files','2'))"
    ).collect()[0]

    assert result[0] == 6  # rewritten
    assert result[1] == 1  # added
    assert count_of(iceberg, maintenance, "files") == 1
    assert iceberg.table(maintenance).count() == 6


def test_compaction_frees_nothing_until_snapshots_expire(
    iceberg: SparkSession, maintenance: str
) -> None:
    """The drill people are surprised by. Compaction adds a snapshot; the old
    files stay on disk because older snapshots still point at them, and a
    table that is only ever compacted grows."""
    name = maintenance.split(".", 1)[1]
    first = (
        iceberg.sql(f"SELECT snapshot_id FROM {maintenance}.snapshots ORDER BY committed_at")
        .collect()[0]
        .snapshot_id
    )

    iceberg.sql(
        f"CALL lake.system.rewrite_data_files(table => '{name}', "
        f"options => map('min-input-files','2'))"
    ).collect()
    # Still readable, so the files it needs were not deleted.
    assert (
        iceberg.sql(f"SELECT count(*) c FROM {maintenance} VERSION AS OF {first}").collect()[0].c
        == 1
    )

    expired = iceberg.sql(
        f"CALL lake.system.expire_snapshots(table => '{name}', "
        f"older_than => TIMESTAMP '2099-01-01 00:00:00', retain_last => 1)"
    ).collect()[0]

    assert expired[0] == 6, "the six pre-compaction files should be deleted"
    assert count_of(iceberg, maintenance, "snapshots") == 1
    assert iceberg.table(maintenance).count() == 6
    # And the history is gone with them.
    with pytest.raises(IllegalArgumentException) as caught:
        iceberg.sql(f"SELECT count(*) FROM {maintenance} VERSION AS OF {first}").collect()

    assert "Cannot find snapshot with ID" in str(caught.value)


def test_rollback_moves_the_table_and_set_current_moves_it_back(
    iceberg: SparkSession, maintenance: str
) -> None:
    """The drill: put the table back to an earlier state, then undo that.

    rollback_to_snapshot only walks backwards - the target has to be an
    ancestor of the current snapshot - so returning needs
    set_current_snapshot. Reaching for the wrong one is how a recovery turns
    into a second incident."""
    name = maintenance.split(".", 1)[1]
    history = [
        r.snapshot_id
        for r in iceberg.sql(
            f"SELECT snapshot_id FROM {maintenance}.snapshots ORDER BY committed_at"
        ).collect()
    ]

    iceberg.sql(f"CALL lake.system.rollback_to_snapshot('{name}', {history[0]})")
    assert iceberg.table(maintenance).count() == 1

    with pytest.raises(Py4JJavaError):
        iceberg.sql(f"CALL lake.system.rollback_to_snapshot('{name}', {history[-1]})")

    iceberg.sql(f"CALL lake.system.set_current_snapshot('{name}', {history[-1]})")
    assert iceberg.table(maintenance).count() == 6


# --- Drill 6: swap the state store -----------------------------------------

HDFS_STORE = "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider"
ROCKSDB_STORE = "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"


def state_memory(spark: SparkSession, provider: str, checkpoint: Path) -> tuple[float, float]:
    """Hold the same number of keys in state and ask what it cost.

    The numbers come from metrics_from, which is the listener's own extraction
    from PR 16 - the drill is measured with the instrument the pipeline
    already carries rather than with a separate one."""
    previous = spark.conf.get("spark.sql.streaming.stateStore.providerClass") or HDFS_STORE
    spark.conf.set("spark.sql.streaming.stateStore.providerClass", provider)
    try:
        source = (
            spark.readStream.format("rate")
            .option("rowsPerSecond", "500")
            .load()
            .withWatermark("timestamp", "5 minutes")
        )
        query = (
            source.groupBy(F.window("timestamp", "30 seconds"), (F.col("value") % 20000).alias("k"))
            .count()
            .writeStream.outputMode("append")
            .format("noop")
            .option("checkpointLocation", str(checkpoint))
            .start()
        )
        try:
            # Bounded. Every other drill reads a file that already exists; this
            # one waits on a live source, and an unbounded wait on a stalled
            # source hangs the suite with nothing to show for it.
            deadline = time.monotonic() + 120
            while query.lastProgress is None or query.lastProgress["batchId"] < 3:
                if time.monotonic() > deadline:
                    pytest.fail(f"{provider} produced no third batch within 120s")
                query.awaitTermination(2)
            metrics = metrics_from(json.loads(query.lastProgress.json))
        finally:
            query.stop()
    finally:
        spark.conf.set("spark.sql.streaming.stateStore.providerClass", previous)
    return metrics["state.0.rows"], metrics["state.0.memory_bytes"]


def test_rocksdb_holds_the_same_state_for_less_memory(
    iceberg: SparkSession, tmp_path: Path
) -> None:
    """The drill: run the same stateful query under both state stores.

    The default keeps state in JVM maps on the executor heap, so a large join
    window is a heap problem and shows up as GC pressure before it shows up
    as anything else. RocksDB keeps it off-heap and spills to disk, which
    trades some latency for a bound. The reported figures differ by enough
    that the shape is not in doubt."""
    default_rows, default_bytes = state_memory(iceberg, HDFS_STORE, tmp_path / "hdfs")
    rocks_rows, rocks_bytes = state_memory(iceberg, ROCKSDB_STORE, tmp_path / "rocks")

    assert default_rows > 0 and rocks_rows > 0
    # Comparing memory is only meaningful if both runs held about the same
    # state. A rate source is driven by the clock, so this is the one drill
    # whose inputs are not identical by construction.
    assert abs(rocks_rows - default_rows) <= 0.25 * default_rows, (
        f"state sizes too different to compare: {default_rows} vs {rocks_rows}"
    )
    assert rocks_bytes < default_bytes


def test_a_field_inserted_rather_than_appended_corrupts_silently(
    iceberg: SparkSession,
) -> None:
    """The case that makes "writer ahead is safe" a narrower claim than it
    sounds.

    Appending is safe because a positional reader stops early. Inserting is
    not: every field after the new one has moved, and the reader has no way
    to know. What comes back is not an error."""
    body = encoded(iceberg, V2_INSERTED, "shp-1", "express", "carrier-a")

    rows = decoded(body, V1)

    # No exception, and carrier now holds what the writer put in
    # service_level. The record is wrong and nothing anywhere says so.
    assert rows == [{"shipment_id": "shp-1", "carrier": "express"}]
