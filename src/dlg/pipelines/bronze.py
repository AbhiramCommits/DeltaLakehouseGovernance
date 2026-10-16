"""Bronze (raw ingest) layer.

Design decisions that matter:

* **No ``inferSchema``.**  Landing files are read against the explicit
  ``StructType`` in :mod:`dlg.schemas`.  Footer/missing columns therefore cannot
  reshape the table.
* **Schema enforcement is real.**  ``spark.databricks.delta.schema.autoMerge``
  is ``false`` session-wide, so a writer that does not match the registered
  schema fails rather than silently widening the table.
* **Quarantine, don't drop.**  Malformed records surface through
  ``_corrupt_record`` (see the column-pruning note in :mod:`dlg.spark`) and
  null/blank business keys are split out before the write; both land in
  ``bronze.quarantine`` with a ``_reject_reason`` and the original payload.
* **Idempotent.**  Every bronze row carries a deterministic ``_record_hash``
  (SHA-256 over the raw payload + relative source file), and the write is a
  ``MERGE ... WHEN NOT MATCHED THEN INSERT``, so re-running ingest over the same
  landing files inserts exactly zero new rows.
"""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from dlg.config import Settings, get_settings
from dlg.lakehouse import ensure_table, hash_expr, merge_upsert, new_batch_id
from dlg.schemas import (
    BRONZE_SCHEMAS,
    CORRUPT_COLUMN,
    QUARANTINE_SCHEMA,
    RAW_SCHEMAS,
)

__all__ = ["BronzeResult", "ingest_dataset", "run_bronze", "DATASETS"]

#: dataset name -> (landing sub-directory, format)
DATASETS: dict[str, tuple[str, str]] = {
    "sessions": ("sessions", "json"),
    "match_events": ("match_events", "json"),
    "purchases": ("purchases", "csv"),
}

#: business keys that must be present on every raw record
REQUIRED_KEYS: dict[str, tuple[str, ...]] = {
    "sessions": ("player_id", "session_id"),
    "match_events": ("event_id", "player_id"),
    "purchases": ("purchase_id", "player_id"),
}

QUARANTINE_TABLE = "bronze.quarantine"


@dataclass
class BronzeResult:
    """Measured outcome of one bronze ingest for one dataset."""

    dataset: str
    source_dir: str
    batch_id: str
    source_files: int
    raw_rows: int
    parse_error_rows: int
    null_key_rows: int
    valid_rows: int
    duplicate_rows_removed: int
    distinct_rows: int
    inserted_rows: int
    table_before: int
    table_after: int
    quarantine_before: int
    quarantine_after: int
    quarantine_by_reason: dict[str, int] = field(default_factory=dict)
    seconds: float = 0.0

    def as_dict(self) -> dict:
        return asdict(self)


def _read_landing(spark: SparkSession, path: str, dataset: str, fmt: str) -> DataFrame:
    """Read landing files against the explicit raw schema, capturing bad rows."""
    schema = RAW_SCHEMAS[dataset]
    reader = (
        spark.read.schema(schema)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COLUMN)
        .option("inferSchema", "false")
    )
    if fmt == "csv":
        reader = reader.option("header", "true").option("enforceSchema", "false")
        return reader.csv(path)
    return reader.json(path)


def _relative_source_file(df: DataFrame) -> DataFrame:
    """A machine-independent ``<dataset>/<file>`` provenance string."""
    return df.withColumn(
        "_source_file", F.regexp_extract(F.input_file_name(), r"([^/]+/[^/]+)$", 1)
    )


def _null_key_condition(dataset: str) -> F.Column:
    cond = None
    for key in REQUIRED_KEYS[dataset]:
        clause = F.col(key).isNull() | (F.trim(F.col(key).cast("string")) == "")
        cond = clause if cond is None else (cond | clause)
    return cond  # type: ignore[return-value]


def _quarantine_frame(
    df: DataFrame,
    dataset: str,
    batch_id: str,
    reason: str,
    detail_expr: F.Column,
    raw_expr: F.Column,
) -> DataFrame:
    """Shape rejected rows into the shared quarantine schema."""
    return df.select(
        F.lit(dataset).alias("_dataset"),
        F.lit(reason).alias("_reject_reason"),
        detail_expr.alias("_reject_detail"),
        F.col("_source_file"),
        F.lit(batch_id).alias("_batch_id"),
        F.current_timestamp().alias("_quarantine_ts"),
        F.sha2(
            F.concat_ws(
                "||",
                F.lit(dataset),
                F.lit(reason),
                F.coalesce(raw_expr.cast("string"), F.lit("")),
                F.coalesce(F.col("_source_file"), F.lit("")),
            ),
            256,
        ).alias("_record_hash"),
        raw_expr.cast("string").alias("_raw_record"),
    )


def ingest_dataset(
    spark: SparkSession,
    dataset: str,
    *,
    batch_id: str | None = None,
    source_subdir: str | None = None,
    settings: Settings | None = None,
) -> BronzeResult:
    """Ingest one landing dataset into ``bronze.<dataset>`` (idempotent)."""
    started = time.perf_counter()
    settings = settings or get_settings()
    batch_id = batch_id or new_batch_id(f"bronze_{dataset}")
    subdir, fmt = DATASETS[dataset]
    source_dir = f"{settings.landing}/{source_subdir or subdir}"

    table_name = f"bronze.{dataset}"
    bronze_schema: StructType = BRONZE_SCHEMAS[dataset]
    ensure_table(spark, table_name, bronze_schema, cdf=True, settings=settings)
    ensure_table(spark, QUARANTINE_TABLE, QUARANTINE_SCHEMA, cdf=True, settings=settings)

    table_before = spark.table(table_name).count()
    quarantine_before = spark.table(QUARANTINE_TABLE).count()

    raw = _relative_source_file(_read_landing(spark, source_dir, dataset, fmt))
    # Spark forbids querying only `_corrupt_record` from a raw file source, so the
    # parsed frame is materialised once (this also avoids re-reading the files for
    # every count below).
    raw = raw.persist()
    raw_rows = raw.count()

    raw_cols = [f.name for f in RAW_SCHEMAS[dataset].fields if f.name != CORRUPT_COLUMN]

    # ---- split out malformed records ------------------------------------
    corrupt = raw.filter(F.col(CORRUPT_COLUMN).isNotNull())
    parsed = raw.filter(F.col(CORRUPT_COLUMN).isNull())

    # ---- split out rows with unusable business keys ---------------------
    bad_keys = parsed.filter(_null_key_condition(dataset))
    valid = parsed.filter(~_null_key_condition(dataset)).drop(CORRUPT_COLUMN)

    # ---- deterministic row identity + dedup ------------------------------
    valid = valid.withColumn("_record_hash", hash_expr([*raw_cols, "_source_file"], prefix=dataset))
    deduped = valid.dropDuplicates(["_record_hash"])
    deduped = deduped.withColumn("_ingest_ts", F.current_timestamp()).withColumn(
        "_batch_id", F.lit(batch_id)
    )
    deduped = deduped.select(*[f.name for f in bronze_schema.fields])

    # ---- counts (real, measured) ----------------------------------------
    parse_error_rows = corrupt.count()
    null_key_rows = bad_keys.count()
    valid_rows = valid.count()
    distinct_rows = deduped.count()
    duplicate_rows_removed = valid_rows - distinct_rows

    q_frames = []
    if parse_error_rows:
        q_frames.append(
            _quarantine_frame(
                corrupt,
                dataset,
                batch_id,
                "PARSE_ERROR",
                F.lit(f"malformed {fmt} record; parser could not bind it to the raw schema"),
                F.col(CORRUPT_COLUMN),
            )
        )
    if null_key_rows:
        q_frames.append(
            _quarantine_frame(
                bad_keys,
                dataset,
                batch_id,
                "NULL_KEY",
                F.lit("null or blank business key: " + ", ".join(REQUIRED_KEYS[dataset])),
                F.to_json(F.struct(*[F.col(c) for c in raw_cols])),
            )
        )

    by_reason: dict[str, int] = {}
    if q_frames:
        q = q_frames[0]
        for extra in q_frames[1:]:
            q = q.unionByName(extra)
        by_reason = {
            r["_reject_reason"]: r["count"] for r in q.groupBy("_reject_reason").count().collect()
        }
        merge_upsert(spark, QUARANTINE_TABLE, q, on=["_record_hash"])

    # ---- idempotent upsert ----------------------------------------------
    merge_upsert(spark, table_name, deduped, on=["_record_hash"])

    table_after = spark.table(table_name).count()
    quarantine_after = spark.table(QUARANTINE_TABLE).count()

    source_files = len({r["_source_file"] for r in raw.select("_source_file").distinct().collect()})
    raw.unpersist()

    return BronzeResult(
        dataset=dataset,
        source_dir=source_dir,
        batch_id=batch_id,
        source_files=source_files,
        raw_rows=raw_rows,
        parse_error_rows=parse_error_rows,
        null_key_rows=null_key_rows,
        valid_rows=valid_rows,
        duplicate_rows_removed=duplicate_rows_removed,
        distinct_rows=distinct_rows,
        inserted_rows=table_after - table_before,
        table_before=table_before,
        table_after=table_after,
        quarantine_before=quarantine_before,
        quarantine_after=quarantine_after,
        quarantine_by_reason=by_reason,
        seconds=round(time.perf_counter() - started, 2),
    )


def run_bronze(
    spark: SparkSession,
    *,
    settings: Settings | None = None,
    include_updates: bool = True,
    datasets: tuple[str, ...] = ("sessions", "match_events", "purchases"),
) -> dict[str, BronzeResult]:
    """Ingest every landing dataset in one batch id."""
    settings = settings or get_settings()
    batch_id = new_batch_id("bronze")
    results: dict[str, BronzeResult] = {}
    for dataset in datasets:
        results[dataset] = ingest_dataset(spark, dataset, batch_id=batch_id, settings=settings)
    if include_updates:
        # Late-arriving revision batch -> same bronze.sessions table.
        results["sessions_updates"] = ingest_dataset(
            spark,
            "sessions",
            batch_id=batch_id,
            source_subdir="sessions_updates",
            settings=settings,
        )
    return results


def print_report(results: dict[str, BronzeResult]) -> None:
    """Print the measured bronze report (used by the CLI and CI logs)."""
    from tabulate import tabulate

    rows = []
    for key, r in results.items():
        rows.append(
            [
                key,
                r.source_files,
                f"{r.raw_rows:,}",
                f"{r.parse_error_rows:,}",
                f"{r.null_key_rows:,}",
                f"{r.duplicate_rows_removed:,}",
                f"{r.inserted_rows:,}",
                f"{r.table_after:,}",
                f"{r.seconds:.2f}",
            ]
        )
    headers = [
        "dataset",
        "files",
        "raw",
        "parse_err",
        "null_key",
        "dups_dropped",
        "inserted",
        "table_rows",
        "secs",
    ]
    print(tabulate(rows, headers=headers, tablefmt="github"))
