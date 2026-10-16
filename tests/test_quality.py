"""Every expectation type, pass and fail paths."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from dlg.quality.expectations import Expectation, evaluate, load_suite_file, set_now

SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("value", IntegerType(), True),
        StructField("code", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("ts", TimestampType(), True),
    ]
)

STAMP = datetime(2025, 9, 1, 10, 0, 0)
ROWS = [
    ("a", 1, "gold", 10.0, STAMP),
    ("b", 2, "silver", 20.0, STAMP),
    ("c", 3, "gold", 30.0, STAMP),
    ("d", None, "gold", None, STAMP),
    ("a", 5, "bronze", 50.0, STAMP),  # duplicate id
]


@pytest.fixture
def table(spark, tmp_table):
    identifier, path = tmp_table("dq", SCHEMA)
    spark.createDataFrame(ROWS, SCHEMA).write.format("delta").mode("append").save(path)
    return identifier


@pytest.fixture
def check(spark):
    def _check(table: str, **kwargs):
        return evaluate(spark, Expectation.from_dict(table, kwargs), run_id="test-run")

    return _check


def test_not_null_pass_and_fail(table, check):
    passing = check(table, kind="not_null", column="id")
    assert passing.status == "PASS"
    assert passing.rows_checked == 5
    assert passing.rows_failed == 0

    failing = check(table, kind="not_null", column="value", max_fail_rate=0.0)
    assert failing.status == "FAIL"
    assert failing.rows_failed == 1
    assert failing.fail_rate == pytest.approx(0.2)


def test_not_null_respects_the_threshold(table, check):
    assert check(table, kind="not_null", column="value", max_fail_rate=0.25).status == "PASS"


def test_unique_detects_duplicates(table, check):
    duplicate_code = check(table, kind="unique", columns=["code"])
    assert duplicate_code.status == "FAIL"
    assert duplicate_code.rows_failed == 2  # 3 golds -> 2 extra rows

    assert check(table, kind="unique", columns=["value"]).status == "PASS"


def test_accepted_values(table, check):
    ok = check(table, kind="accepted_values", column="code", values=["gold", "silver", "bronze"])
    assert ok.status == "PASS"
    assert ok.rows_checked == 5

    bad = check(table, kind="accepted_values", column="code", values=["gold"])
    assert bad.status == "FAIL"
    assert bad.rows_failed == 2


def test_range_excludes_nulls_and_flags_outliers(table, check):
    ok = check(table, kind="range", column="value", min_value=1, max_value=5)
    assert ok.status == "PASS"
    assert ok.rows_checked == 4  # the NULL is excluded

    bad = check(table, kind="range", column="value", min_value=2, max_value=3)
    assert bad.status == "FAIL"
    assert bad.rows_failed == 2  # the values 1 and 5


def test_regex(table, check):
    assert check(table, kind="regex", column="code", pattern="^[a-z]+$").status == "PASS"
    bad = check(table, kind="regex", column="code", pattern="^g.*")
    assert bad.status == "FAIL"
    assert bad.rows_failed == 2


def test_row_count_between(table, check):
    ok = check(table, kind="row_count_between", min_rows=1, max_rows=10)
    assert ok.status == "PASS"
    assert ok.rows_checked == 5
    assert check(table, kind="row_count_between", min_rows=100).status == "FAIL"


def test_referential_integrity(spark, table, check, tmp_table):
    ref_schema = StructType([StructField("code", StringType(), True)])
    ref_id, ref_path = tmp_table("ref", ref_schema)
    spark.createDataFrame([("gold",), ("silver",)], ref_schema).write.format("delta").mode(
        "append"
    ).save(ref_path)

    result = check(
        table,
        kind="referential_integrity",
        column="code",
        ref_table=ref_id,
        ref_column="code",
    )
    assert result.status == "FAIL"
    assert result.rows_failed == 1  # "bronze"
    assert result.rows_checked == 5


def test_freshness_within_pass_and_fail(table, check):
    set_now(datetime(2025, 9, 1, 10, 30, tzinfo=UTC))
    ok = check(table, kind="freshness_within", column="ts", max_staleness_minutes=60)
    assert ok.status == "PASS", ok.detail

    set_now(datetime(2025, 9, 3, 10, 0, tzinfo=UTC))
    stale = check(table, kind="freshness_within", column="ts", max_staleness_minutes=60)
    assert stale.status == "FAIL"
    assert "minutes old" in stale.detail
    set_now(None)


def test_unknown_kind_and_severity_are_rejected(table):
    with pytest.raises(ValueError):
        Expectation(table=table, kind="not_a_kind", column="id")
    with pytest.raises(ValueError):
        Expectation(table=table, kind="not_null", column="id", severity="LOUD")


def test_suites_file_covers_silver_and_gold():
    path = Path(__file__).resolve().parents[1] / "src" / "dlg" / "quality" / "suites.yml"
    expectations, meta = load_suite_file(str(path))
    tables = {e.table for e in expectations}
    assert "silver.sessions" in tables
    assert "gold.fct_player_daily" in tables
    assert len(expectations) >= 40
    assert meta["version"] == 1


def test_exactly_one_deliberate_warn_expectation_fails(spark, lakehouse):
    from dlg.quality.runner import run_quality

    run = run_quality(spark, run_id="pytest-dq")
    assert run.blocking_failures == []
    warn = run.warn_failures
    assert len(warn) == 1
    assert warn[0].expectation == "raw_session_start_ts_is_iso8601"
    assert warn[0].rows_failed > 0
    assert run.pass_rate < 1.0


def test_dq_results_are_persisted(spark, lakehouse):
    from dlg.quality.runner import results_frame, run_quality

    run = run_quality(spark, run_id="pytest-dq-persist")
    persisted = results_frame(spark)
    assert persisted.count() == run.total
    statuses = {r["status"] for r in persisted.select("status").distinct().collect()}
    assert statuses <= {"PASS", "FAIL"}
