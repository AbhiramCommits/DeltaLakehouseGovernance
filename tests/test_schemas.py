"""Schema declarations are explicit and internally consistent."""

from __future__ import annotations

import pytest
from pyspark.sql.types import StructType

from dlg.schemas import (
    BRONZE_SCHEMAS,
    BUSINESS_KEYS,
    CORRUPT_COLUMN,
    GOLD_SCHEMAS,
    INGEST_METADATA,
    QUARANTINE_SCHEMA,
    RAW_SCHEMAS,
    REJECT_REASONS,
    SILVER_SCHEMAS,
)


def test_raw_schemas_are_structtypes_with_corrupt_column():
    for name, schema in RAW_SCHEMAS.items():
        assert isinstance(schema, StructType)
        assert CORRUPT_COLUMN in schema.names, name
        assert len(schema.fields) > 5, name


def test_bronze_schema_is_raw_minus_corrupt_plus_metadata():
    for name, raw in RAW_SCHEMAS.items():
        bronze = BRONZE_SCHEMAS[name]
        expected = [f.name for f in raw.fields if f.name != CORRUPT_COLUMN]
        expected += [f.name for f in INGEST_METADATA.fields]
        assert bronze.names == expected, name
        assert CORRUPT_COLUMN not in bronze.names


def test_ingest_metadata_columns_are_not_nullable():
    for field in INGEST_METADATA.fields:
        assert not field.nullable, field.name


def test_business_keys_are_present_and_not_nullable_in_silver():
    for dataset, keys in BUSINESS_KEYS.items():
        schema = SILVER_SCHEMAS[dataset]
        for key in keys:
            assert key in schema.names, (dataset, key)
            assert not schema[key].nullable, (dataset, key)


def test_silver_tables_carry_event_date_and_provenance():
    for name, schema in SILVER_SCHEMAS.items():
        assert "event_date" in schema.names, name
        assert "event_ts" in schema.names, name
        for column in ("_record_hash", "_ingest_ts", "_batch_id"):
            assert column in schema.names, (name, column)


@pytest.mark.parametrize("table", sorted(GOLD_SCHEMAS))
def test_gold_schemas_are_explicit(table):
    schema = GOLD_SCHEMAS[table]
    assert isinstance(schema, StructType)
    assert schema.names


def test_quarantine_schema_supports_every_reject_reason():
    assert "_reject_reason" in QUARANTINE_SCHEMA.names
    assert "_dataset" in QUARANTINE_SCHEMA.names
    assert "_record_hash" in QUARANTINE_SCHEMA.names
    assert len(REJECT_REASONS) >= 6


def test_silver_schema_types_are_typed_not_strings():
    sessions = SILVER_SCHEMAS["sessions"]
    assert sessions["session_start_ts"].dataType.simpleString() == "timestamp"
    assert sessions["event_date"].dataType.simpleString() == "date"
    assert sessions["birth_year"].dataType.simpleString() == "int"
    assert sessions["duration_minutes"].dataType.simpleString() == "int"
