"""Catalog validation and drift detection."""

from __future__ import annotations

import copy

import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from dlg.governance.catalog import (
    Catalog,
    CatalogTable,
    ColumnSpec,
    SlaBlock,
    catalog_summary,
    load_catalog,
    validate_catalog,
)


def test_catalog_loads_every_table():
    catalog = load_catalog()
    assert catalog.version == 1
    for expected in (
        "bronze.sessions",
        "silver.sessions",
        "silver.purchases",
        "gold.fct_player_daily",
        "gold.dim_player",
    ):
        assert expected in catalog
    summary = catalog_summary(catalog)
    assert summary["tables"] >= 12
    assert summary["columns"] > 100
    assert summary["pii_columns"] > 0


def test_pii_columns_are_declared_on_the_right_tables():
    catalog = load_catalog()
    assert "player_email" in catalog["silver.sessions"].pii_columns
    assert "player_ip" in catalog["silver.sessions"].pii_columns
    assert "birth_year" in catalog["silver.sessions"].pii_columns
    assert "country" in catalog["silver.sessions"].pii_columns
    assert "payment_method" in catalog["silver.purchases"].pii_columns


def test_catalog_matches_the_live_delta_schemas(spark, lakehouse):
    report = validate_catalog(spark, require_all=True)
    assert report.ok, report.render()
    assert len(report.checked_tables) >= 12


def _table_with(columns: dict[str, object], upstream: list[str] | None = None) -> CatalogTable:
    return CatalogTable(
        name="t",
        owner="o",
        domain="d",
        layer="gold",
        description="",
        classification="INTERNAL",
        upstream=upstream or [],
        sla=SlaBlock(),
        columns={
            name: ColumnSpec(name=name, type=type_, description="", classification="INTERNAL")
            for name, type_ in columns.items()
        },
    )


def test_undeclared_physical_column_is_detected(spark, tmp_table):
    """The test the spec calls for: an undeclared column must fail validation."""
    schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("sneaky_new_column", IntegerType(), True),
        ]
    )
    identifier, _ = tmp_table("drift", schema)

    catalog = Catalog(
        version=1, name="t", domains={}, tables={"test_drift": _table_with({"id": "string"})}
    )
    # point the catalog's table name at the temp table by copying it across
    catalog.tables[str(identifier)] = catalog.tables.pop("test_drift")

    report = validate_catalog(spark, catalog)
    kinds = {issue.kind for issue in report.issues}
    assert "UNDECLARED_COLUMN" in kinds
    assert not report.ok
    assert any(i.column == "sneaky_new_column" for i in report.issues)


def test_declared_but_missing_column_is_detected(spark, tmp_table):
    schema = StructType([StructField("id", StringType(), False)])
    identifier, _ = tmp_table("stale", schema)
    catalog = Catalog(
        version=1,
        name="t",
        domains={},
        tables={
            str(identifier): _table_with({"id": "string", "gone": "int"}),
        },
    )
    report = validate_catalog(spark, catalog)
    assert any(i.kind == "MISSING_COLUMN" and i.column == "gone" for i in report.issues)


def test_type_mismatch_is_detected(spark, tmp_table):
    schema = StructType([StructField("id", StringType(), False)])
    identifier, _ = tmp_table("types", schema)
    catalog = Catalog(
        version=1,
        name="t",
        domains={},
        tables={str(identifier): _table_with({"id": "int"})},
    )
    report = validate_catalog(spark, catalog)
    assert any(i.kind == "TYPE_MISMATCH" for i in report.issues)


def test_invalid_classification_is_rejected_at_load(tmp_path):
    bad = tmp_path / "catalog.yml"
    bad.write_text(
        """
version: 1
tables:
  gold.x:
    owner: o
    columns:
      a: { type: string, description: d, classification: SECRET }
"""
    )
    with pytest.raises(ValueError) as exc:
        load_catalog(bad)
    assert "classification" in str(exc.value).lower()


def test_missing_table_is_reported_when_required(spark):
    catalog = load_catalog()
    trimmed = copy.deepcopy(catalog)
    trimmed.tables["gold.does_not_exist"] = _table_with({"a": "string"})
    report = validate_catalog(spark, trimmed, require_all=True)
    assert "gold.does_not_exist" in report.missing_tables
    assert not report.ok
