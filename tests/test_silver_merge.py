"""Silver MERGE semantics: idempotency and late-arriving data."""

from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import functions as F

from dlg.lakehouse import ddl_for, merge_upsert
from dlg.schemas import SILVER_SCHEMAS


@pytest.fixture
def silver_target(spark, tmp_table):
    """A throwaway silver.sessions-shaped table plus a row builder."""
    schema = SILVER_SCHEMAS["sessions"]
    identifier, path = tmp_table("sessions", schema, partition_by=["event_date"])

    def row(session_id: str, event_ts: datetime, duration: int = 10):
        return {
            "session_id": session_id,
            "player_id": "P000001",
            "player_email": "a@example.com",
            "player_ip": "10.0.0.1",
            "country": "US",
            "region": "NA",
            "birth_year": 1990,
            "device": "pc",
            "platform": "pc",
            "app_version": "1.4.0",
            "session_start_ts": event_ts,
            "session_end_ts": event_ts,
            "event_ts": event_ts,
            "event_date": event_ts.date(),
            "duration_minutes": duration,
            "matches_played": 1,
            "kills": 1,
            "deaths": 1,
            "store_views": 0,
            "_record_hash": f"h_{session_id}_{duration}",
            "_ingest_ts": event_ts,
            "_batch_id": "b1",
        }

    def frame(rows):
        return spark.createDataFrame(rows, schema)

    return {"identifier": identifier, "path": path, "row": row, "frame": frame}


def _duration(spark, identifier, session_id):
    rows = spark.table(identifier).filter(F.col("session_id") == session_id).collect()
    return rows[0]["duration_minutes"] if rows else None


def test_merge_inserts_then_replaying_is_a_noop(spark, silver_target):
    target = silver_target["identifier"]
    ts = datetime(2025, 9, 1, 10, 0, 0)
    batch = silver_target["frame"]([silver_target["row"]("S1", ts), silver_target["row"]("S2", ts)])

    merge_upsert(
        spark,
        target,
        batch,
        on=["player_id", "session_id"],
        update_condition="s.event_ts > t.event_ts",
    )
    assert spark.table(target).count() == 2

    metrics = merge_upsert(
        spark,
        target,
        batch,
        on=["player_id", "session_id"],
        update_condition="s.event_ts > t.event_ts",
    )
    assert spark.table(target).count() == 2
    assert metrics.get("num_inserted_rows", 0) == 0


def test_late_arriving_newer_event_ts_wins(spark, silver_target):
    target = silver_target["identifier"]
    ts = datetime(2025, 9, 1, 10, 0, 0)
    merge_upsert(
        spark,
        target,
        silver_target["frame"]([silver_target["row"]("S1", ts, 10)]),
        on=["player_id", "session_id"],
        update_condition="s.event_ts > t.event_ts",
    )
    newer = ts.replace(hour=12)
    merge_upsert(
        spark,
        target,
        silver_target["frame"]([silver_target["row"]("S1", newer, 42)]),
        on=["player_id", "session_id"],
        update_condition="s.event_ts > t.event_ts",
    )
    assert _duration(spark, target, "S1") == 42
    assert spark.table(target).count() == 1


def test_late_arriving_older_event_ts_is_ignored(spark, silver_target):
    target = silver_target["identifier"]
    ts = datetime(2025, 9, 1, 12, 0, 0)
    merge_upsert(
        spark,
        target,
        silver_target["frame"]([silver_target["row"]("S1", ts, 10)]),
        on=["player_id", "session_id"],
        update_condition="s.event_ts > t.event_ts",
    )
    older = ts.replace(hour=9)
    merge_upsert(
        spark,
        target,
        silver_target["frame"]([silver_target["row"]("S1", older, 99)]),
        on=["player_id", "session_id"],
        update_condition="s.event_ts > t.event_ts",
    )
    assert _duration(spark, target, "S1") == 10, "a stale revision must not overwrite newer data"
    assert spark.table(target).count() == 1


def test_silver_tables_are_partitioned_and_cdf_enabled(spark, lakehouse):
    detail = spark.sql("DESCRIBE DETAIL silver.sessions").collect()[0]
    assert detail["partitionColumns"] == ["event_date"]
    assert str(detail["properties"]).find("delta.enableChangeDataFeed") >= 0


def test_silver_double_run_produces_identical_counts(spark, lakehouse):
    from dlg.pipelines.silver import run_silver

    before = {
        t: spark.table(f"silver.{t}").count() for t in ("sessions", "match_events", "purchases")
    }
    results = run_silver(spark)
    after = {
        t: spark.table(f"silver.{t}").count() for t in ("sessions", "match_events", "purchases")
    }
    assert before == after
    for name, result in results.items():
        assert result.inserted_rows == 0, name
        assert result.updated_rows == 0, name


def test_change_data_feed_is_readable(spark, lakehouse):
    cdf = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", "0")
        .table("silver.purchases")
    )
    assert "_change_type" in cdf.columns
    assert "_commit_version" in cdf.columns
    assert cdf.count() > 0


def test_ddl_for_matches_the_declared_schema(spark, silver_target):
    physical = {
        f.name: f.dataType.simpleString()
        for f in spark.table(silver_target["identifier"]).schema.fields
    }
    declared = {f.name: f.dataType.simpleString() for f in SILVER_SCHEMAS["sessions"].fields}
    assert physical == declared
    assert ddl_for("x", SILVER_SCHEMAS["sessions"], "/tmp/x").startswith(
        "CREATE TABLE IF NOT EXISTS x"
    )
