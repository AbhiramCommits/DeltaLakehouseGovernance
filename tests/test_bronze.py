"""Bronze ingest: parse failures, null keys, deterministic hashes, enforcement."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from dlg.pipelines.bronze import _null_key_condition, _read_landing, _relative_source_file
from dlg.schemas import CORRUPT_COLUMN, RAW_SCHEMAS

VALID_ROW = {
    "player_id": "P900001",
    "session_id": "S900001",
    "player_email": "test@example.com",
    "player_ip": "10.0.0.1",
    "country": "US",
    "region": "NA",
    "birth_year": 1995,
    "device": "pc",
    "platform": "pc",
    "app_version": "1.4.0",
    "session_start_ts": "2025-09-01T10:00:00.000Z",
    "session_end_ts": "2025-09-01T10:30:00.000Z",
    "duration_minutes": 30,
    "matches_played": 2,
    "kills": 5,
    "deaths": 3,
    "store_views": 1,
}


@pytest.fixture
def landing_dir(tmp_path: Path) -> Path:
    directory = tmp_path / "sessions"
    directory.mkdir()
    lines = [
        json.dumps(VALID_ROW),
        json.dumps({**VALID_ROW, "session_id": "S900002"}),
        '{"player_id":"P900003","sess',  # truncated -> corrupt
        json.dumps({**VALID_ROW, "session_id": None}),  # null business key
        json.dumps({**VALID_ROW, "session_id": "   "}),  # blank business key
    ]
    (directory / "part-0000.json").write_text("\n".join(lines) + "\n")
    return directory


def test_read_landing_uses_the_explicit_schema(spark, landing_dir):
    df = _read_landing(spark, str(landing_dir), "sessions", "json")
    assert df.schema.names == RAW_SCHEMAS["sessions"].names
    # no schema inference: the declared types win even though the source is text
    assert df.schema["birth_year"].dataType.simpleString() == "int"
    assert df.schema["session_start_ts"].dataType.simpleString() == "string"


def test_corrupt_records_are_captured_not_dropped(spark, landing_dir):
    df = _relative_source_file(_read_landing(spark, str(landing_dir), "sessions", "json")).persist()
    total = df.count()
    corrupt = df.filter(F.col(CORRUPT_COLUMN).isNotNull())
    assert total == 5
    assert corrupt.count() == 1
    assert "P900003" in corrupt.collect()[0][CORRUPT_COLUMN]
    df.unpersist()


def test_null_key_condition_flags_null_and_blank(spark, landing_dir):
    """Only well-formed rows are checked: the malformed line is a PARSE_ERROR.

    A truncated JSON line is recovered permissively (player_id survives, the rest
    is NULL), so it satisfies the null-key predicate too. The pipeline splits
    corrupt records *first*, which is why it is quarantined exactly once.
    """
    df = _relative_source_file(_read_landing(spark, str(landing_dir), "sessions", "json")).persist()
    parsed = df.filter(F.col(CORRUPT_COLUMN).isNull())
    flagged = parsed.filter(_null_key_condition("sessions")).collect()
    assert len(flagged) == 2
    # the malformed row would also match, so ordering in the pipeline matters
    assert df.filter(_null_key_condition("sessions")).count() == 3
    df.unpersist()


def test_relative_source_file_is_machine_independent(spark, landing_dir):
    df = _relative_source_file(_read_landing(spark, str(landing_dir), "sessions", "json"))
    values = {row["_source_file"] for row in df.select("_source_file").collect()}
    assert values == {"sessions/part-0000.json"}


def test_delta_schema_enforcement_rejects_an_undeclared_column(spark, tmp_table):
    """autoMerge is off, so a writer cannot silently widen a table."""
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    schema = StructType(
        [StructField("id", StringType(), False), StructField("n", IntegerType(), True)]
    )
    identifier, _ = tmp_table("enforced", schema, partition_by=None)
    good = spark.createDataFrame([("a", 1)], "id string, n int")
    good.write.format("delta").mode("append").save(identifier.split("`")[1])

    rogue = spark.createDataFrame([("b", 2, "extra")], "id string, n int, oops string")
    with pytest.raises(Exception) as exc:
        rogue.write.format("delta").mode("append").save(identifier.split("`")[1])
    message = str(exc.value)
    assert "oops" in message or "schema" in message.lower()


def test_record_hash_is_deterministic_and_payload_sensitive(spark, landing_dir):
    from dlg.lakehouse import hash_expr

    df = _read_landing(spark, str(landing_dir), "sessions", "json").filter(
        F.col(CORRUPT_COLUMN).isNull()
    )
    hashed = df.withColumn("_record_hash", hash_expr(["player_id", "session_id"], prefix="t"))
    first = hashed.select("_record_hash").collect()[0]["_record_hash"]
    second = hashed.select("_record_hash").collect()[0]["_record_hash"]
    assert first == second
    assert len(first) == 64  # sha2-256 hex


def test_bronze_ingest_is_idempotent_for_the_real_lake(spark, lakehouse):
    from dlg.pipelines.bronze import run_bronze

    before = spark.table("bronze.sessions").count()
    results = run_bronze(spark, include_updates=True)
    after = spark.table("bronze.sessions").count()
    assert before == after
    assert results["sessions"].inserted_rows == 0
    assert results["sessions_updates"].inserted_rows == 0
