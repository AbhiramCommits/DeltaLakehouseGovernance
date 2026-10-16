"""SLA evaluation: staleness, volume and null-rate breaches."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

from dlg.governance.catalog import Catalog, CatalogTable, ColumnSpec, SlaBlock
from dlg.quality.slas import run_sla_checks


def _catalog(name: str, sla: SlaBlock, columns: dict[str, str]) -> Catalog:
    table = CatalogTable(
        name=name,
        owner="o",
        domain="d",
        layer=name.split(".")[0],
        description="",
        classification="INTERNAL",
        upstream=[],
        sla=sla,
        columns={
            c: ColumnSpec(name=c, type=t, description="", classification="INTERNAL")
            for c, t in columns.items()
        },
    )
    return Catalog(version=1, name="t", domains={}, tables={name: table})


def test_staleness_breach_is_detected(spark, tmp_table):
    """A table whose newest ingest timestamp is 10 days old breaches a 1-day SLA."""
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("_ingest_ts", TimestampType(), True),
        ]
    )
    identifier, path = tmp_table("stale_sla", schema)
    old = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=10)
    spark.createDataFrame([("a", old)], schema).write.format("delta").mode("append").save(path)

    catalog = _catalog(
        identifier,
        SlaBlock(max_staleness_minutes=1440, min_daily_rows=0, max_null_rate=1.0),
        {"id": "string"},
    )
    run = run_sla_checks(spark, catalog=catalog, persist=False)
    staleness = next(r for r in run.results if r.sla_name == "max_staleness_minutes")
    assert staleness.status == "BREACH"
    assert staleness.measured > 1440
    assert run.breaches


def test_fresh_table_passes_staleness(spark, tmp_table):
    schema = StructType(
        [StructField("id", StringType(), True), StructField("_ingest_ts", TimestampType(), True)]
    )
    identifier, path = tmp_table("fresh_sla", schema)
    now = datetime.now(UTC).replace(tzinfo=None)
    spark.createDataFrame([("a", now)], schema).write.format("delta").mode("append").save(path)
    catalog = _catalog(
        identifier,
        SlaBlock(max_staleness_minutes=60, min_daily_rows=0, max_null_rate=1.0),
        {"id": "string"},
    )
    run = run_sla_checks(spark, catalog=catalog, persist=False)
    assert run.breaches == []


def test_min_daily_rows_breach_is_detected(spark, tmp_table):
    schema = StructType([StructField("id", StringType(), True)])
    identifier, path = tmp_table("volume_sla", schema)
    spark.createDataFrame([("a",), ("b",)], schema).write.format("delta").mode("append").save(path)
    catalog = _catalog(
        identifier,
        SlaBlock(max_staleness_minutes=10**9, min_daily_rows=1000, max_null_rate=1.0),
        {"id": "string"},
    )
    run = run_sla_checks(spark, catalog=catalog, persist=False)
    volume = next(r for r in run.results if r.sla_name == "min_daily_rows")
    assert volume.status == "BREACH"
    assert volume.measured == pytest.approx(2.0)


def test_null_rate_breach_is_detected(spark, tmp_table):
    schema = StructType(
        [StructField("id", StringType(), True), StructField("amount", IntegerType(), True)]
    )
    identifier, path = tmp_table("nulls_sla", schema)
    spark.createDataFrame([("a", None), ("b", None), ("c", 1)], schema).write.format("delta").mode(
        "append"
    ).save(path)
    catalog = _catalog(
        identifier,
        SlaBlock(max_staleness_minutes=10**9, min_daily_rows=0, max_null_rate=0.1),
        {"id": "string", "amount": "int"},
    )
    run = run_sla_checks(spark, catalog=catalog, persist=False)
    nulls = next(r for r in run.results if r.sla_name == "max_null_rate")
    assert nulls.status == "BREACH"
    assert nulls.measured == pytest.approx(2 / 3)
    assert "amount" in nulls.detail


def test_audit_only_table_null_rate_is_not_applicable(spark, tmp_table):
    """Quarantine-style tables have only audit columns: no spurious 100% breach."""
    schema = StructType(
        [StructField("_reason", StringType(), True), StructField("_detail", StringType(), True)]
    )
    identifier, path = tmp_table("audit_sla", schema)
    spark.createDataFrame([("a", None)], schema).write.format("delta").mode("append").save(path)
    catalog = _catalog(
        identifier,
        SlaBlock(max_staleness_minutes=10**9, min_daily_rows=0, max_null_rate=0.0),
        {"_reason": "string", "_detail": "string"},
    )
    run = run_sla_checks(spark, catalog=catalog, persist=False)
    nulls = next(r for r in run.results if r.sla_name == "max_null_rate")
    assert nulls.status == "N/A"
    assert run.breaches == []


def test_real_lake_sla_status_is_persisted(spark, lakehouse):
    run = run_sla_checks(spark)
    assert len(run.results) >= 30
    persisted = spark.table("ops.sla_status")
    assert persisted.count() == len(run.results)
    statuses = {r["status"] for r in persisted.select("status").distinct().collect()}
    assert statuses <= {"OK", "BREACH", "N/A"}
