"""Explicit schemas for every table in the lakehouse.

Nothing in this repository uses ``inferSchema``.  Landing files are read with the
``RAW_*`` schemas below so that malformed records surface as ``_corrupt_record``
values instead of silently reshaping the table, and every downstream writer uses
an explicit ``StructType`` so Delta's schema enforcement is a real guardrail.
"""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

CORRUPT_COLUMN = "_corrupt_record"

# ======================================================================
# Landing / raw schemas (what the generator writes)
# ======================================================================

RAW_SESSIONS = StructType(
    [
        StructField("player_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("player_email", StringType(), True),
        StructField("player_ip", StringType(), True),
        StructField("country", StringType(), True),
        StructField("region", StringType(), True),
        StructField("birth_year", IntegerType(), True),
        StructField("device", StringType(), True),
        StructField("platform", StringType(), True),
        StructField("app_version", StringType(), True),
        StructField("session_start_ts", StringType(), True),
        StructField("session_end_ts", StringType(), True),
        StructField("duration_minutes", IntegerType(), True),
        StructField("matches_played", IntegerType(), True),
        StructField("kills", IntegerType(), True),
        StructField("deaths", IntegerType(), True),
        StructField("store_views", IntegerType(), True),
        StructField(CORRUPT_COLUMN, StringType(), True),
    ]
)

RAW_MATCH_EVENTS = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("player_id", StringType(), True),
        StructField("match_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_ts", StringType(), True),
        StructField("map_id", StringType(), True),
        StructField("mode", StringType(), True),
        StructField("kills", IntegerType(), True),
        StructField("deaths", IntegerType(), True),
        StructField("damage_dealt", IntegerType(), True),
        StructField("ping_ms", IntegerType(), True),
        StructField("is_bot", BooleanType(), True),
        StructField(CORRUPT_COLUMN, StringType(), True),
    ]
)

RAW_PURCHASES = StructType(
    [
        StructField("purchase_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("player_id", StringType(), True),
        StructField("sku", StringType(), True),
        StructField("item_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price_usd", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("purchase_ts", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField(CORRUPT_COLUMN, StringType(), True),
    ]
)

RAW_SCHEMAS: dict[str, StructType] = {
    "sessions": RAW_SESSIONS,
    "match_events": RAW_MATCH_EVENTS,
    "purchases": RAW_PURCHASES,
}

# ======================================================================
# Ingestion metadata columns added by the bronze layer
# ======================================================================

INGEST_METADATA = StructType(
    [
        StructField("_ingest_ts", TimestampType(), False),
        StructField("_source_file", StringType(), False),
        StructField("_batch_id", StringType(), False),
        StructField("_record_hash", StringType(), False),
    ]
)

# ======================================================================
# Bronze tables: raw columns + ingest metadata
# ======================================================================


def _with_metadata(schema: StructType) -> StructType:
    fields = [f for f in schema.fields if f.name != CORRUPT_COLUMN]
    return StructType(fields + list(INGEST_METADATA.fields))


BRONZE_SESSIONS = _with_metadata(RAW_SESSIONS)
BRONZE_MATCH_EVENTS = _with_metadata(RAW_MATCH_EVENTS)
BRONZE_PURCHASES = _with_metadata(RAW_PURCHASES)

BRONZE_SCHEMAS: dict[str, StructType] = {
    "sessions": BRONZE_SESSIONS,
    "match_events": BRONZE_MATCH_EVENTS,
    "purchases": BRONZE_PURCHASES,
}

#: Reason codes written to ``*_quarantine`` tables.
REJECT_REASONS = (
    "PARSE_ERROR",
    "NULL_KEY",
    "BAD_TIMESTAMP",
    "OUT_OF_RANGE",
    "UNKNOWN_PLAYER",
    "NEGATIVE_AMOUNT",
    "DUPLICATE_KEY",
    "SILVER_VALIDATION",
)

QUARANTINE_SCHEMA = StructType(
    [
        StructField("_dataset", StringType(), False),
        StructField("_reject_reason", StringType(), False),
        StructField("_reject_detail", StringType(), True),
        StructField("_source_file", StringType(), True),
        StructField("_batch_id", StringType(), True),
        StructField("_quarantine_ts", TimestampType(), False),
        StructField("_record_hash", StringType(), False),
        StructField("_raw_record", StringType(), True),
    ]
)

# ======================================================================
# Silver schemas (typed, deduplicated, business-key keyed)
# ======================================================================

SILVER_SESSIONS = StructType(
    [
        StructField("session_id", StringType(), False),
        StructField("player_id", StringType(), False),
        StructField("player_email", StringType(), True),
        StructField("player_ip", StringType(), True),
        StructField("country", StringType(), True),
        StructField("region", StringType(), True),
        StructField("birth_year", IntegerType(), True),
        StructField("device", StringType(), True),
        StructField("platform", StringType(), True),
        StructField("app_version", StringType(), True),
        StructField("session_start_ts", TimestampType(), False),
        StructField("session_end_ts", TimestampType(), False),
        StructField("event_ts", TimestampType(), False),
        StructField("event_date", DateType(), False),
        StructField("duration_minutes", IntegerType(), True),
        StructField("matches_played", IntegerType(), True),
        StructField("kills", IntegerType(), True),
        StructField("deaths", IntegerType(), True),
        StructField("store_views", IntegerType(), True),
        StructField("_record_hash", StringType(), False),
        StructField("_ingest_ts", TimestampType(), False),
        StructField("_batch_id", StringType(), False),
    ]
)

SILVER_MATCH_EVENTS = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("player_id", StringType(), False),
        StructField("match_id", StringType(), True),
        StructField("event_type", StringType(), False),
        StructField("event_ts", TimestampType(), False),
        StructField("event_date", DateType(), False),
        StructField("map_id", StringType(), True),
        StructField("mode", StringType(), True),
        StructField("kills", IntegerType(), True),
        StructField("deaths", IntegerType(), True),
        StructField("damage_dealt", IntegerType(), True),
        StructField("ping_ms", IntegerType(), True),
        StructField("is_bot", BooleanType(), True),
        StructField("_record_hash", StringType(), False),
        StructField("_ingest_ts", TimestampType(), False),
        StructField("_batch_id", StringType(), False),
    ]
)

SILVER_PURCHASES = StructType(
    [
        StructField("purchase_id", StringType(), False),
        StructField("session_id", StringType(), True),
        StructField("player_id", StringType(), False),
        StructField("sku", StringType(), True),
        StructField("item_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price_usd", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("purchase_ts", TimestampType(), False),
        StructField("event_ts", TimestampType(), False),
        StructField("event_date", DateType(), False),
        StructField("payment_method", StringType(), True),
        StructField("_record_hash", StringType(), False),
        StructField("_ingest_ts", TimestampType(), False),
        StructField("_batch_id", StringType(), False),
    ]
)

SILVER_SCHEMAS: dict[str, StructType] = {
    "sessions": SILVER_SESSIONS,
    "match_events": SILVER_MATCH_EVENTS,
    "purchases": SILVER_PURCHASES,
}

#: Business keys used by the silver MERGE statements.
BUSINESS_KEYS: dict[str, tuple[str, ...]] = {
    "sessions": ("player_id", "session_id"),
    "match_events": ("event_id",),
    "purchases": ("purchase_id",),
}

# ======================================================================
# Gold schemas
# ======================================================================

GOLD_FCT_PLAYER_DAILY = StructType(
    [
        StructField("event_date", DateType(), False),
        StructField("player_id", StringType(), False),
        StructField("country", StringType(), True),
        StructField("region", StringType(), True),
        StructField("sessions", LongType(), False),
        StructField("playtime_minutes", LongType(), False),
        StructField("matches", LongType(), False),
        StructField("kills", LongType(), False),
        StructField("deaths", LongType(), False),
        StructField("spend_usd", DoubleType(), False),
    ]
)

GOLD_DIM_PLAYER = StructType(
    [
        StructField("player_id", StringType(), False),
        StructField("first_seen", TimestampType(), False),
        StructField("last_seen", TimestampType(), False),
        StructField("country", StringType(), True),
        StructField("region", StringType(), True),
        StructField("lifetime_spend", DoubleType(), False),
        StructField("lifetime_sessions", LongType(), False),
        StructField("lifetime_playtime_minutes", LongType(), False),
        StructField("days_since_last_seen", IntegerType(), False),
        StructField("churn_flag", BooleanType(), False),
    ]
)

GOLD_FCT_MONETIZATION_DAILY = StructType(
    [
        StructField("event_date", DateType(), False),
        StructField("dau", LongType(), False),
        StructField("paying_users", LongType(), False),
        StructField("revenue_usd", DoubleType(), False),
        StructField("arpdau", DoubleType(), False),
        StructField("conversion_rate", DoubleType(), False),
        StructField("sessions", LongType(), False),
        StructField("purchases", LongType(), False),
    ]
)

GOLD_FCT_RETENTION_COHORT = StructType(
    [
        StructField("cohort_date", DateType(), False),
        StructField("cohort_size", LongType(), False),
        StructField("d1_retained", LongType(), False),
        StructField("d7_retained", LongType(), False),
        StructField("d30_retained", LongType(), False),
        StructField("d1_retention", DoubleType(), False),
        StructField("d7_retention", DoubleType(), False),
        StructField("d30_retention", DoubleType(), False),
    ]
)

GOLD_SCHEMAS: dict[str, StructType] = {
    "fct_player_daily": GOLD_FCT_PLAYER_DAILY,
    "dim_player": GOLD_DIM_PLAYER,
    "fct_monetization_daily": GOLD_FCT_MONETIZATION_DAILY,
    "fct_retention_cohort": GOLD_FCT_RETENTION_COHORT,
}
