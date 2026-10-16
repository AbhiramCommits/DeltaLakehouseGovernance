"""Silver (cleansed, typed, deduplicated) layer.

The silver layer is where bad data becomes *explicit*:

* timestamps are parsed with an explicit pattern -- unparseable values become
  ``NULL`` and are routed to ``silver.quarantine`` with ``BAD_TIMESTAMP``;
* numeric ranges are validated (``OUT_OF_RANGE``), purchase amounts must be
  positive (``NEGATIVE_AMOUNT``), and purchases must reference a known player
  (``UNKNOWN_PLAYER``, a real referential-integrity check against
  ``silver.sessions``);
* the surviving rows are deduplicated **by business key**, keeping the newest
  ``event_ts``, and written with ``MERGE INTO``.

The MERGE carries a guarded UPDATE branch (``s.event_ts > t.event_ts``), which is
what makes the layer idempotent *and* correct for late-arriving data: replaying
the same batch updates nothing, and a stale revision loses to the row already
stored.
"""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from dlg.config import Settings, get_settings
from dlg.lakehouse import ensure_table, merge_upsert, new_batch_id
from dlg.schemas import BUSINESS_KEYS, QUARANTINE_SCHEMA, SILVER_SCHEMAS

__all__ = [
    "SilverResult",
    "build_dataset",
    "run_silver",
    "print_report",
    "SILVER_TABLES",
    "QUARANTINE_TABLE",
    "VALID_EVENT_TYPES",
    "VALID_PLATFORMS",
    "VALID_CATEGORIES",
]

SILVER_TABLES = ("sessions", "match_events", "purchases")
QUARANTINE_TABLE = "silver.quarantine"

#: explicit timestamp patterns tried in order (ISO-8601 with/without millis)
TS_FORMATS = ("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "yyyy-MM-dd'T'HH:mm:ss'Z'")

#: accepted operator values (shared with the DQ ``accepted_values`` expectations)
VALID_EVENT_TYPES = ("kill", "death", "objective", "match_end")
VALID_PLATFORMS = ("mobile", "pc", "console")
VALID_CATEGORIES = ("battle_pass", "skin", "currency", "bundle", "emote")

MIN_BIRTH_YEAR = 1900
MIN_WORKING_AGE = 5  # a player younger than this in the data is not plausible


@dataclass
class SilverResult:
    """Measured outcome of one silver build for one dataset."""

    dataset: str
    batch_id: str
    bronze_rows: int
    valid_rows: int
    dedup_rows_removed: int
    reject_rows: int
    reject_by_reason: dict[str, int] = field(default_factory=dict)
    inserted_rows: int = 0
    updated_rows: int = 0
    merge_metrics: dict[str, int] = field(default_factory=dict)
    table_before: int = 0
    table_after: int = 0
    quarantine_after: int = 0
    seconds: float = 0.0

    def as_dict(self) -> dict:
        return asdict(self)


def _parse_ts(col: str) -> F.Column:
    """Parse an ISO-8601 string with trailing ``Z``; NULL when unparseable."""
    return F.coalesce(*[F.to_timestamp(F.col(col), fmt) for fmt in TS_FORMATS])


def _clean(df: DataFrame, dataset: str) -> DataFrame:
    """Project bronze columns onto the typed silver shape (pre-validation)."""
    if dataset == "sessions":
        return (
            df.withColumn("session_start_ts", _parse_ts("session_start_ts"))
            .withColumn("session_end_ts", _parse_ts("session_end_ts"))
            .withColumn("event_ts", _parse_ts("session_start_ts"))
        )
    if dataset == "match_events":
        return df.withColumn("event_ts", _parse_ts("event_ts"))
    return df.withColumn("purchase_ts", _parse_ts("purchase_ts")).withColumn(
        "event_ts", _parse_ts("purchase_ts")
    )


def _reject_reason(dataset: str, current_year: int) -> F.Column:
    """A single column expression yielding the reject reason (or NULL)."""
    if dataset == "sessions":
        return (
            F.when(
                F.col("session_start_ts").isNull() | F.col("session_end_ts").isNull(),
                F.lit("BAD_TIMESTAMP"),
            )
            .when(
                F.col("session_start_ts") < F.lit("2020-01-01").cast("timestamp"),
                F.lit("OUT_OF_RANGE"),
            )
            .when(
                F.col("session_end_ts") > F.lit(f"{current_year + 1}-01-01").cast("timestamp"),
                F.lit("OUT_OF_RANGE"),
            )
            .when(
                F.col("duration_minutes").isNull()
                | (F.col("duration_minutes") < 1)
                | (F.col("duration_minutes") > 1440),
                F.lit("OUT_OF_RANGE"),
            )
            .when(
                F.col("kills").isNull() | (F.col("kills") < 0) | (F.col("kills") > 500),
                F.lit("OUT_OF_RANGE"),
            )
            .when(
                F.col("deaths").isNull() | (F.col("deaths") < 0) | (F.col("deaths") > 500),
                F.lit("OUT_OF_RANGE"),
            )
            .when(
                F.col("matches_played").isNull()
                | (F.col("matches_played") < 0)
                | (F.col("matches_played") > 100),
                F.lit("OUT_OF_RANGE"),
            )
            .when(
                F.col("store_views").isNull()
                | (F.col("store_views") < 0)
                | (F.col("store_views") > 100),
                F.lit("OUT_OF_RANGE"),
            )
            .when(
                F.col("birth_year").isNotNull()
                & (
                    (F.col("birth_year") < MIN_BIRTH_YEAR)
                    | (F.col("birth_year") > current_year - MIN_WORKING_AGE)
                ),
                F.lit("OUT_OF_RANGE"),
            )
            .when(
                F.col("platform").isNull() | ~F.col("platform").isin(*VALID_PLATFORMS),
                F.lit("SILVER_VALIDATION"),
            )
        )
    if dataset == "match_events":
        return (
            F.when(F.col("event_ts").isNull(), F.lit("BAD_TIMESTAMP"))
            .when(
                F.col("event_type").isNull() | ~F.col("event_type").isin(*VALID_EVENT_TYPES),
                F.lit("SILVER_VALIDATION"),
            )
            .when(
                F.col("ping_ms").isNull() | (F.col("ping_ms") < 0) | (F.col("ping_ms") > 5000),
                F.lit("OUT_OF_RANGE"),
            )
            .when(
                F.col("damage_dealt").isNull()
                | (F.col("damage_dealt") < 0)
                | (F.col("damage_dealt") > 100_000),
                F.lit("OUT_OF_RANGE"),
            )
            .when(
                F.col("kills").isNotNull() & ((F.col("kills") < 0) | (F.col("kills") > 1)),
                F.lit("OUT_OF_RANGE"),
            )
            .when(
                F.col("deaths").isNotNull() & ((F.col("deaths") < 0) | (F.col("deaths") > 1)),
                F.lit("OUT_OF_RANGE"),
            )
        )
    return (
        F.when(F.col("purchase_ts").isNull(), F.lit("BAD_TIMESTAMP"))
        .when(F.col("price_usd").isNull() | (F.col("price_usd") <= 0), F.lit("NEGATIVE_AMOUNT"))
        .when(F.col("price_usd") > 1000, F.lit("OUT_OF_RANGE"))
        .when(
            F.col("quantity").isNull() | (F.col("quantity") < 1) | (F.col("quantity") > 10),
            F.lit("OUT_OF_RANGE"),
        )
        .when(
            F.col("category").isNull() | ~F.col("category").isin(*VALID_CATEGORIES),
            F.lit("SILVER_VALIDATION"),
        )
    )


def build_dataset(
    spark: SparkSession,
    dataset: str,
    *,
    batch_id: str | None = None,
    settings: Settings | None = None,
    current_year: int | None = None,
) -> SilverResult:
    """Cleanse + MERGE one bronze table into its silver counterpart."""
    started = time.perf_counter()
    settings = settings or get_settings()
    batch_id = batch_id or new_batch_id(f"silver_{dataset}")
    current_year = current_year or datetime.now(UTC).year

    source_table = f"bronze.{dataset}"
    target_table = f"silver.{dataset}"
    schema: StructType = SILVER_SCHEMAS[dataset]
    keys = BUSINESS_KEYS[dataset]

    ensure_table(
        spark, target_table, schema, partition_by=["event_date"], cdf=True, settings=settings
    )
    ensure_table(spark, QUARANTINE_TABLE, QUARANTINE_SCHEMA, cdf=True, settings=settings)

    table_before = spark.table(target_table).count()
    bronze_rows = spark.table(source_table).count()

    cleaned = _clean(spark.table(source_table), dataset)
    cleaned = cleaned.withColumn("_reject_reason", _reject_reason(dataset, current_year))
    cleaned = cleaned.withColumn(
        "_reject_detail",
        F.concat_ws(" | ", F.col("_reject_reason"), F.lit(f"{dataset} failed silver validation")),
    )

    # ---- referential integrity: purchases must reference a known player ----
    if dataset == "purchases":
        known = spark.table("silver.sessions").select("player_id").distinct()
        unknown_ids = (
            cleaned.filter(F.col("_reject_reason").isNull())
            .select("purchase_id", "player_id")
            .join(known, on="player_id", how="left_anti")
            .select("purchase_id")
            .withColumn("_unknown", F.lit(True))
        )
        cleaned = (
            cleaned.join(unknown_ids, on="purchase_id", how="left")
            .withColumn(
                "_reject_reason",
                # NB: PySpark 3.5 has no Column.isTrue()/isFalse() -- those exist
                # only on Spark Connect.  Unknown attributes fall through
                # Column.__getattr__ and become column lookups, so the call fails
                # with "Column object is not callable".  The join flag is TRUE or
                # NULL, so isNotNull() is the portable test.
                F.when(F.col("_unknown").isNotNull(), F.lit("UNKNOWN_PLAYER")).otherwise(
                    F.col("_reject_reason")
                ),
            )
            .withColumn(
                "_reject_detail",
                F.when(
                    F.col("_reject_reason") == "UNKNOWN_PLAYER",
                    F.lit("UNKNOWN_PLAYER | player_id has no row in silver.sessions"),
                ).otherwise(F.col("_reject_detail")),
            )
            .drop("_unknown")
        )

    cleaned = cleaned.persist()

    rejects = cleaned.filter(F.col("_reject_reason").isNotNull())
    valid = cleaned.filter(F.col("_reject_reason").isNull()).withColumn(
        "event_date", F.to_date("event_ts")
    )

    # ---- dedup by business key, keeping the newest event_ts ----------------
    window = Window.partitionBy(*keys).orderBy(
        F.col("event_ts").desc_nulls_last(), F.col("_ingest_ts").desc()
    )
    deduped = (
        valid.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("_batch_id", F.lit(batch_id))
        .select(*[f.name for f in schema.fields])
        .persist()
    )

    valid_rows = valid.count()
    dedup_rows_removed = valid_rows - deduped.count()
    reject_rows = rejects.count()
    by_reason = (
        {
            r["_reject_reason"]: r["count"]
            for r in rejects.groupBy("_reject_reason").count().collect()
        }
        if reject_rows
        else {}
    )

    # ---- quarantine (idempotent MERGE on a derived hash) -------------------
    if reject_rows:
        raw_cols = [c for c in spark.table(source_table).columns if not c.startswith("_")]
        q = rejects.select(
            F.lit(dataset).alias("_dataset"),
            F.col("_reject_reason"),
            F.col("_reject_detail"),
            F.lit(None).cast("string").alias("_source_file"),
            F.col("_batch_id").cast("string"),
            F.current_timestamp().alias("_quarantine_ts"),
            F.sha2(
                F.concat_ws(
                    "||",
                    F.lit(dataset),
                    F.col("_reject_reason"),
                    F.coalesce(F.col("_record_hash"), F.lit("")),
                ),
                256,
            ).alias("_record_hash"),
            F.to_json(F.struct(*[F.col(c) for c in raw_cols])).alias("_raw_record"),
        )
        merge_upsert(spark, QUARANTINE_TABLE, q, on=["_record_hash"])

    # ---- idempotent upsert with a guarded UPDATE branch --------------------
    metrics = merge_upsert(
        spark,
        target_table,
        deduped,
        on=list(keys),
        update_condition="s.event_ts > t.event_ts",
    )

    table_after = spark.table(target_table).count()
    result = SilverResult(
        dataset=dataset,
        batch_id=batch_id,
        bronze_rows=bronze_rows,
        valid_rows=valid_rows,
        dedup_rows_removed=dedup_rows_removed,
        reject_rows=reject_rows,
        reject_by_reason=by_reason,
        inserted_rows=metrics.get("num_inserted_rows", 0),
        updated_rows=metrics.get("num_updated_rows", 0),
        merge_metrics=metrics,
        table_before=table_before,
        table_after=table_after,
        quarantine_after=spark.table(QUARANTINE_TABLE).count(),
        seconds=round(time.perf_counter() - started, 2),
    )

    cleaned.unpersist()
    deduped.unpersist()
    return result


def run_silver(
    spark: SparkSession,
    *,
    settings: Settings | None = None,
    datasets: tuple[str, ...] = SILVER_TABLES,
) -> dict[str, SilverResult]:
    """Build all silver tables. Order matters: sessions first (RI check)."""
    settings = settings or get_settings()
    batch_id = new_batch_id("silver")
    return {
        dataset: build_dataset(spark, dataset, batch_id=batch_id, settings=settings)
        for dataset in datasets
    }


def print_report(results: dict[str, SilverResult]) -> None:
    from tabulate import tabulate

    rows = []
    for key, r in results.items():
        rows.append(
            [
                key,
                f"{r.bronze_rows:,}",
                f"{r.valid_rows:,}",
                f"{r.dedup_rows_removed:,}",
                f"{r.reject_rows:,}",
                f"{r.inserted_rows:,}",
                f"{r.updated_rows:,}",
                f"{r.table_before:,}",
                f"{r.table_after:,}",
                f"{r.seconds:.2f}",
            ]
        )
    headers = [
        "dataset",
        "bronze_rows",
        "valid",
        "dedup_dropped",
        "rejected",
        "inserted",
        "updated",
        "before",
        "after",
        "secs",
    ]
    print(tabulate(rows, headers=headers, tablefmt="github"))

    reason_rows = []
    for key, r in results.items():
        for reason, count in sorted(r.reject_by_reason.items()):
            reason_rows.append([key, reason, f"{count:,}"])
    if reason_rows:
        print("\nquarantine reasons:")
        print(tabulate(reason_rows, headers=["dataset", "reason", "rows"], tablefmt="github"))
