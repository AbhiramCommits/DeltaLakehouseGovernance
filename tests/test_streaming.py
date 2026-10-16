"""Streaming ingest: --once processing, duplicate suppression and replay guard."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from dlg.pipelines.stream import (
    BRONZE_STREAM_TABLE,
    SILVER_STREAM_TABLE,
    STREAM_SOURCE_SUBDIR,
    apply_micro_batch,
    replay_last_batch,
    run_stream,
)
from dlg.schemas import RAW_SCHEMAS

VALID = {
    "player_id": "P800001",
    "session_id": "S800001",
    "player_email": "s@example.com",
    "player_ip": "10.0.0.9",
    "country": "US",
    "region": "NA",
    "birth_year": 1990,
    "device": "pc",
    "platform": "pc",
    "app_version": "1.4.0",
    "session_start_ts": "2025-09-01T10:00:00.000Z",
    "session_end_ts": "2025-09-01T10:20:00.000Z",
    "duration_minutes": 20,
    "matches_played": 1,
    "kills": 3,
    "deaths": 2,
    "store_views": 0,
}


def _row(index: int) -> dict:
    return {**VALID, "session_id": f"S800{index:03d}", "player_id": f"P800{index:03d}"}


@pytest.fixture
def stream_env(spark, settings):
    """A clean, isolated streaming source directory plus clean streaming tables.

    ``stream_subdir`` is overridden on a copy of the settings so the test tails its
    own directory instead of the batch landing zone the session fixture generated --
    the same isolation ``scripts/stream_demo.sh`` gets from ``DLG_STREAM_SUBDIR``.
    """
    settings = settings.model_copy(update={"stream_subdir": STREAM_SOURCE_SUBDIR})
    directory = Path(settings.landing) / STREAM_SOURCE_SUBDIR
    shutil.rmtree(directory, ignore_errors=True)
    directory.mkdir(parents=True, exist_ok=True)
    checkpoint = Path(settings.checkpoints) / "sessions_stream"
    shutil.rmtree(checkpoint, ignore_errors=True)
    for table in (BRONZE_STREAM_TABLE, SILVER_STREAM_TABLE, "ops.stream_batches"):
        shutil.rmtree(Path(settings.lake_path(*table.split("."))), ignore_errors=True)
        spark.sql(f"DROP TABLE IF EXISTS {table}")

    def write(name: str, rows: list[dict]) -> Path:
        path = directory / name
        path.write_text("\n".join(json.dumps(r) for r in rows) + "\n")
        return path

    yield {"dir": directory, "write": write, "settings": settings}
    shutil.rmtree(directory, ignore_errors=True)
    shutil.rmtree(checkpoint, ignore_errors=True)


def test_once_ingests_available_files(spark, stream_env):
    stream_env["write"]("batch-a.json", [_row(i) for i in range(1, 6)])
    results = run_stream(spark, mode="once", settings=stream_env["settings"])
    assert len(results) == 1
    assert results[0].silver_inserted == 5
    assert spark.table(SILVER_STREAM_TABLE).count() == 5
    assert spark.table(BRONZE_STREAM_TABLE).count() == 5


def test_duplicate_file_adds_zero_rows(spark, stream_env):
    stream_env["write"]("batch-a.json", [_row(i) for i in range(1, 6)])
    run_stream(spark, mode="once", settings=stream_env["settings"])
    before = spark.table(SILVER_STREAM_TABLE).count()

    # byte-identical copy under a different name
    original = stream_env["dir"] / "batch-a.json"
    shutil.copyfile(original, stream_env["dir"] / "copy-of-batch-a.json")
    run_stream(spark, mode="once", settings=stream_env["settings"])

    assert spark.table(SILVER_STREAM_TABLE).count() == before


def test_new_files_after_a_duplicate_still_land(spark, stream_env):
    stream_env["write"]("batch-a.json", [_row(i) for i in range(1, 4)])
    run_stream(spark, mode="once", settings=stream_env["settings"])

    stream_env["write"]("batch-b.json", [_row(i) for i in range(10, 13)])
    run_stream(spark, mode="once", settings=stream_env["settings"])

    assert spark.table(SILVER_STREAM_TABLE).count() == 6


def test_replay_of_last_batch_is_a_noop(spark, stream_env):
    stream_env["write"]("batch-a.json", [_row(i) for i in range(1, 4)])
    run_stream(spark, mode="once", settings=stream_env["settings"])
    before = spark.table(SILVER_STREAM_TABLE).count()

    replay = replay_last_batch(spark, stream_env["settings"])
    assert replay.skipped_replay is True
    assert replay.silver_inserted == 0
    assert spark.table(SILVER_STREAM_TABLE).count() == before


def test_batch_id_is_content_addressed(spark, stream_env):
    """The same files must produce the same batch id, independent of the query state."""
    stream_env["write"]("batch-a.json", [_row(1)])
    first = run_stream(spark, mode="once", settings=stream_env["settings"])[0]
    replay = replay_last_batch(spark, stream_env["settings"])
    assert first.batch_id == replay.batch_id


def test_bad_records_are_quarantined_with_reasons(spark, stream_env):
    stream_env["write"](
        "batch-mixed.json",
        [
            _row(1),
            {**_row(2), "session_start_ts": "not-a-timestamp", "session_end_ts": "not-a-timestamp"},
            {**_row(3), "duration_minutes": 99999},
        ],
    )
    results = run_stream(spark, mode="once", settings=stream_env["settings"])
    assert results[0].quarantined == 2
    assert spark.table(SILVER_STREAM_TABLE).count() == 1

    reasons = {
        r["_reject_reason"]
        for r in spark.table("bronze.quarantine")
        .filter("_dataset = 'sessions_stream'")
        .select("_reject_reason")
        .distinct()
        .collect()
    }
    assert "BAD_TIMESTAMP" in reasons
    assert "OUT_OF_RANGE" in reasons


def test_corrupt_json_is_quarantined(spark, stream_env):
    path = stream_env["dir"] / "batch-corrupt.json"
    path.write_text(json.dumps(_row(1)) + "\n" + '{"player_id":"P8' + "\n")
    results = run_stream(spark, mode="once", settings=stream_env["settings"])
    assert results[0].quarantined == 1
    assert spark.table(SILVER_STREAM_TABLE).count() == 1


def test_apply_micro_batch_is_idempotent_when_called_directly(spark, stream_env):
    stream_env["write"]("batch-a.json", [_row(i) for i in range(1, 4)])
    frame = (
        spark.read.schema(RAW_SCHEMAS["sessions"])
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(str(stream_env["dir"]))
    )
    first = apply_micro_batch(spark, frame, settings=stream_env["settings"])
    second = apply_micro_batch(spark, frame, settings=stream_env["settings"])
    assert first.silver_inserted == 3
    assert second.skipped_replay is True
    assert second.bronze_inserted == 0
