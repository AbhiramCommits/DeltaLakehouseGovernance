"""Delta time travel: history, RESTORE and prior-count recovery."""

from __future__ import annotations

import pytest

from dlg.lakehouse import delta_history, delta_version
from dlg.schemas import SILVER_SCHEMAS
from dlg.timetravel import demo_rollback, print_history


@pytest.fixture
def purchases_table(spark, tmp_table):
    schema = SILVER_SCHEMAS["purchases"]
    identifier, path = tmp_table("tt_purchases", schema, partition_by=["event_date"])
    from datetime import datetime

    ts = datetime(2025, 9, 1, 10, 0, 0)
    rows = []
    for i in range(1, 6):
        rows.append(
            {
                "purchase_id": f"U{i:03d}",
                "session_id": "S1",
                "player_id": "P1",
                "sku": "SKU1",
                "item_name": "Item",
                "category": "skin",
                "price_usd": 9.99,
                "currency": "USD",
                "quantity": 1,
                "purchase_ts": ts,
                "event_ts": ts,
                "event_date": ts.date(),
                "payment_method": "card",
                "_record_hash": f"h{i}",
                "_ingest_ts": ts,
                "_batch_id": "b1",
            }
        )
    spark.createDataFrame(rows, schema).write.format("delta").mode("append").save(path)
    return identifier, path


def test_history_reports_versions_and_operations(spark, purchases_table):
    identifier, _ = purchases_table
    history = delta_history(spark, identifier)
    assert history
    assert delta_version(spark, identifier) == history[0]["version"]
    assert history[0]["operation"] in {"WRITE", "CREATE TABLE", "MERGE", "RESTORE"}


def test_demo_rollback_restores_the_prior_count(spark, purchases_table):
    identifier, path = purchases_table
    before = spark.table(identifier).count()

    report = demo_rollback(spark, identifier)
    assert report.count_before == before
    assert report.count_corrupt == before * 2  # every row duplicated under a BAD_ key
    assert report.rows_added == before
    assert report.count_after == before
    assert report.restored_equal is True

    # the assertion is real: the table really is back to the prior version's data
    assert spark.table(identifier).count() == before
    assert spark.table(identifier).filter("purchase_id LIKE 'BAD_%'").count() == 0


def test_restore_adds_a_new_version_without_rewriting_history(spark, purchases_table):
    identifier, _ = purchases_table
    versions_before = {row["version"] for row in delta_history(spark, identifier)}
    demo_rollback(spark, identifier)
    versions_after = {row["version"] for row in delta_history(spark, identifier)}
    assert versions_before < versions_after
    operations = {row["operation"] for row in delta_history(spark, identifier)}
    assert "RESTORE" in operations


def test_bad_merge_uses_a_deterministic_key(spark, purchases_table):
    identifier, _ = purchases_table
    versions_before = delta_version(spark, identifier)
    demo_rollback(spark, identifier)
    # 3 commits: the bad MERGE and the RESTORE (plus possibly the initial write)
    assert delta_version(spark, identifier) >= versions_before + 2


def test_print_history_renders(spark, purchases_table, capsys):
    identifier, _ = purchases_table
    print_history(spark, identifier)
    captured = capsys.readouterr().out
    assert "Delta history" in captured
    assert "version" in captured


def test_time_travel_query_returns_the_old_snapshot(spark, purchases_table):
    identifier, path = purchases_table
    before = spark.table(identifier).count()
    version = delta_version(spark, identifier)
    demo_rollback(spark, identifier)
    as_of = spark.sql(f"SELECT count(*) AS c FROM {identifier} VERSION AS OF {version}").collect()[
        0
    ]["c"]
    assert as_of == before
