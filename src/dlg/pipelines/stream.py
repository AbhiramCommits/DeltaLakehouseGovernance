"""Incremental Structured Streaming ingest with idempotent reprocessing.

The query is an Auto-Loader-style file source over the landing directory:

* explicit schema, **no** ``inferSchema``; malformed lines are captured through
  ``_corrupt_record`` (see the column-pruning note in :mod:`dlg.spark`);
* ``maxFilesPerTrigger=2`` so each micro-batch is small and observable;
* ``checkpointLocation`` under ``_checkpoints/``.

Each micro-batch is handled by ``foreachBatch``, which performs three
**idempotent** Delta MERGEs:

1. good rows -> ``bronze.sessions_stream`` on the payload ``_record_hash``
   (byte-identical replays collapse);
2. the same rows, cleansed and typed -> ``silver.sessions_stream`` on the
   ``(player_id, session_id)`` business key with the guarded
   ``s.event_ts > t.event_ts`` UPDATE;
3. rejects (parse errors, null keys) -> ``bronze.quarantine`` with a reason.

Two independent layers make reprocessing a no-op:

* the **file source checkpoint** means an already-processed file is never read
  again;
* the **content-addressed ``_batch_id``** (a hash of the batch's source files)
  is recorded in ``ops.stream_batches``; if a batch id has already been applied
  the whole micro-batch short-circuits. That is what survives a checkpoint
  replay: the dedup key is derived from the *data*, not from the query's state.
"""

from __future__ import annotations

import hashlib
import os
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from dlg.config import Settings, get_settings
from dlg.lakehouse import ensure_table, hash_expr, merge_upsert
from dlg.pipelines.bronze import QUARANTINE_TABLE
from dlg.pipelines.silver import TS_FORMATS, VALID_PLATFORMS
from dlg.schemas import (
    BRONZE_SCHEMAS,
    CORRUPT_COLUMN,
    QUARANTINE_SCHEMA,
    RAW_SCHEMAS,
    SILVER_SCHEMAS,
)

__all__ = [
    "MicroBatchResult",
    "apply_micro_batch",
    "run_stream",
    "replay_last_batch",
    "stream_batches",
    "BRONZE_STREAM_TABLE",
    "SILVER_STREAM_TABLE",
    "BATCH_LEDGER_TABLE",
    "STREAM_SOURCE_SUBDIR",
]

BRONZE_STREAM_TABLE = "bronze.sessions_stream"
SILVER_STREAM_TABLE = "silver.sessions_stream"
BATCH_LEDGER_TABLE = "ops.stream_batches"
#: Isolated landing directory used by scripts/stream_demo.sh (via DLG_STREAM_SUBDIR).
STREAM_SOURCE_SUBDIR = "sessions_stream"

#: keys that must be present for a streamed row to be usable
REQUIRED_KEYS = ("player_id", "session_id")

BATCH_LEDGER_SCHEMA = StructType(
    [
        StructField("batch_id", StringType(), False),
        StructField("ts", TimestampType(), False),
        StructField("source_files", IntegerType(), False),
        StructField("source_file_list", StringType(), True),
        StructField("rows_read", LongType(), False),
        StructField("rows_duplicate_suppressed", LongType(), False),
        StructField("bronze_inserted", LongType(), False),
        StructField("silver_inserted", LongType(), False),
        StructField("silver_updated", LongType(), False),
        StructField("quarantined", LongType(), False),
        StructField("skipped_replay", StringType(), False),
        StructField("latency_ms", LongType(), False),
    ]
)


@dataclass
class MicroBatchResult:
    batch_id: str
    source_files: int
    rows_read: int
    rows_duplicate_suppressed: int = 0
    bronze_inserted: int = 0
    silver_inserted: int = 0
    silver_updated: int = 0
    quarantined: int = 0
    skipped_replay: bool = False
    latency_ms: int = 0
    seconds: float = 0.0
    file_list: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        d = asdict(self)
        d["file_list"] = ";".join(self.file_list)
        return d

    def summary_line(self) -> str:
        state = "REPLAY-SKIPPED" if self.skipped_replay else "APPLIED"
        return (
            f"batch {self.batch_id} [{state}] files={self.source_files} read={self.rows_read} "
            f"bronze+={self.bronze_inserted} silver+={self.silver_inserted} "
            f"silver~={self.silver_updated} dups_suppressed={self.rows_duplicate_suppressed} "
            f"quarantined={self.quarantined} latency={self.latency_ms}ms"
        )


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------


def source_dir(settings: Settings | None = None, subdir: str | None = None) -> str:
    """The landing directory the streaming query tails.

    Defaults to ``settings.stream_subdir`` (the main sessions landing zone), so a
    plain ``make stream`` on a fresh checkout has something to ingest.
    ``scripts/stream_demo.sh`` sets ``DLG_STREAM_SUBDIR=sessions_stream`` to work
    against an isolated directory it fully controls.
    """
    settings = settings or get_settings()
    return f"{settings.landing}/{subdir or settings.stream_subdir}"


def _batch_id_for(files: list[str]) -> str:
    digest = hashlib.sha256("|".join(sorted(files)).encode()).hexdigest()[:16]
    return f"stream_{digest}"


def _file_age_ms(paths: list[str], settings: Settings) -> int:
    """End-to-end latency: how long since the newest source file landed."""
    newest = 0.0
    for path in paths:
        candidate = path
        if path.startswith("file:"):
            candidate = path[len("file:") :]
        try:
            newest = max(newest, os.path.getmtime(candidate))
        except OSError:
            continue
    if not newest:
        return 0
    return int(max(0.0, (time.time() - newest) * 1000))


def _relative_files(df: DataFrame) -> DataFrame:
    """Ensure ``_source_file`` / ``_source_path`` provenance columns are present.

    A streaming file source exposes ``_metadata.file_path``, but that column does
    not survive into ``foreachBatch`` (the batch is a plain DataFrame), so the
    streaming query projects it to real columns up front.  Batch-read replays
    fall back to ``input_file_name()``.
    """
    if "_source_file" in df.columns and "_source_path" in df.columns:
        return df
    source = F.input_file_name()
    return df.withColumn("_source_path", source).withColumn(
        "_source_file", F.regexp_extract(source, r"([^/]+)$", 1)
    )


def _null_key_condition() -> F.Column:
    cond = None
    for key in REQUIRED_KEYS:
        clause = F.col(key).isNull() | (F.trim(F.col(key).cast("string")) == "")
        cond = clause if cond is None else (cond | clause)
    return cond  # type: ignore[return-value]


def _parse_ts(column: str) -> F.Column:
    """Parse an ISO-8601 string with trailing ``Z``; NULL when unparseable."""
    return F.coalesce(*[F.to_timestamp(F.col(column), fmt) for fmt in TS_FORMATS])


def _stream_reject_reason(current_year: int) -> F.Column:
    del current_year  # kept for signature symmetry with the batch silver layer
    start = _parse_ts("session_start_ts")
    end = _parse_ts("session_end_ts")
    return (
        F.when(start.isNull() | end.isNull(), F.lit("BAD_TIMESTAMP"))
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
            F.col("platform").isNull() | ~F.col("platform").isin(*VALID_PLATFORMS),
            F.lit("SILVER_VALIDATION"),
        )
    )


def stream_batches(spark: SparkSession) -> list[dict]:
    """The recorded micro-batch ledger, newest first."""
    try:
        rows = spark.table(BATCH_LEDGER_TABLE).orderBy(F.desc("ts")).collect()
    except Exception:
        return []
    return [r.asDict(recursive=True) for r in rows]


def _batch_applied(spark: SparkSession, batch_id: str) -> bool:
    if not spark.catalog.tableExists(BATCH_LEDGER_TABLE):
        return False
    return (
        spark.table(BATCH_LEDGER_TABLE).filter(F.col("batch_id") == batch_id).limit(1).count() > 0
    )


# --------------------------------------------------------------------------
# the micro-batch handler
# --------------------------------------------------------------------------


def apply_micro_batch(
    spark: SparkSession,
    batch: DataFrame,
    *,
    settings: Settings | None = None,
    current_year: int | None = None,
    force: bool = False,
) -> MicroBatchResult:
    """Idempotently land one micro-batch into bronze, silver and quarantine."""
    started = time.perf_counter()
    settings = settings or get_settings()
    current_year = current_year or datetime.now(UTC).year
    # Inside `foreachBatch` the batch DataFrame carries its own SparkSession;
    # temporary views registered from it are only visible through that session,
    # so every statement below must use it rather than a session captured by the
    # caller. (Temp views created on a foreign session silently fail these
    # MERGEs with TABLE_OR_VIEW_NOT_FOUND.)
    spark = batch.sparkSession

    with_files = _relative_files(batch).persist()
    file_rows = with_files.select("_source_file", "_source_path").distinct().collect()
    file_list = sorted({r["_source_file"] for r in file_rows})
    paths = sorted({r["_source_path"] for r in file_rows})
    batch_id = _batch_id_for(file_list)

    if not force and _batch_applied(spark, batch_id):
        # The batch id is derived from the source files, so a replay lands here
        # and returns without touching bronze, silver or quarantine.
        result = MicroBatchResult(
            batch_id=batch_id,
            source_files=len(file_list),
            rows_read=spark.table(SILVER_STREAM_TABLE).count(),
            skipped_replay=True,
            seconds=round(time.perf_counter() - started, 2),
            file_list=file_list,
        )
        with_files.unpersist()
        return result

    ensure_table(
        spark, BRONZE_STREAM_TABLE, BRONZE_SCHEMAS["sessions"], cdf=True, settings=settings
    )
    ensure_table(
        spark,
        SILVER_STREAM_TABLE,
        SILVER_SCHEMAS["sessions"],
        partition_by=["event_date"],
        cdf=True,
        settings=settings,
    )
    ensure_table(spark, QUARANTINE_TABLE, QUARANTINE_SCHEMA, cdf=True, settings=settings)

    quarantine_before = spark.table(QUARANTINE_TABLE).count()

    rows_read = with_files.count()

    raw_cols = [f.name for f in RAW_SCHEMAS["sessions"].fields if f.name != CORRUPT_COLUMN]

    corrupt = with_files.filter(F.col(CORRUPT_COLUMN).isNotNull())
    parsed = with_files.filter(F.col(CORRUPT_COLUMN).isNull())
    bad_keys = parsed.filter(_null_key_condition())
    good = (
        parsed.filter(~_null_key_condition())
        .drop(CORRUPT_COLUMN)
        # Content-addressed: the hash covers the payload only (not the file name),
        # so a byte-identical replay of a file collapses to the same key.
        .withColumn("_record_hash", hash_expr(raw_cols, prefix="stream"))
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_id))
        .persist()
    )

    # ---- 1. bronze: content-addressed dedupe sink --------------------------
    # A landing file may legitimately contain the same record twice (the generator
    # injects exact resends), and Delta rejects a MERGE with duplicate source keys,
    # so the batch is deduplicated on `_record_hash` first.
    bronze_rows = good.dropDuplicates(["_record_hash"]).select(
        *[f.name for f in BRONZE_SCHEMAS["sessions"].fields]
    )
    bronze_inserted, _ = preview_merge_counts(
        spark, BRONZE_STREAM_TABLE, bronze_rows, ["_record_hash"]
    )
    merge_upsert(spark, BRONZE_STREAM_TABLE, bronze_rows, on=["_record_hash"])

    # ---- 2. silver: typed, validated, deduped, guarded MERGE ---------------
    typed = (
        good.withColumn("session_start_ts", _parse_ts("session_start_ts"))
        .withColumn("session_end_ts", _parse_ts("session_end_ts"))
        .withColumn("event_ts", _parse_ts("session_start_ts"))
    )
    typed = typed.withColumn("_reject_reason", _stream_reject_reason(current_year))
    typed = typed.withColumn(
        "_reject_detail",
        F.concat_ws(
            " | ", F.col("_reject_reason"), F.lit("sessions_stream failed silver validation")
        ),
    )
    typed = typed.persist()
    rejects = typed.filter(F.col("_reject_reason").isNotNull())
    valid = typed.filter(F.col("_reject_reason").isNull()).withColumn(
        "event_date", F.to_date("event_ts")
    )

    # Deduplicate by business key inside the micro-batch, keeping the newest
    # event_ts, exactly as the batch silver layer does.
    key_window = Window.partitionBy("player_id", "session_id").orderBy(
        F.col("event_ts").desc_nulls_last(), F.col("_ingest_ts").desc()
    )
    silver_source = (
        valid.withColumn("_rn", F.row_number().over(key_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .select(*[f.name for f in SILVER_SCHEMAS["sessions"].fields])
    )
    silver_inserted, silver_updated = preview_merge_counts(
        spark,
        SILVER_STREAM_TABLE,
        silver_source,
        ["player_id", "session_id"],
        update_condition="s.event_ts > t.event_ts",
    )
    merge_upsert(
        spark,
        SILVER_STREAM_TABLE,
        silver_source,
        on=["player_id", "session_id"],
        update_condition="s.event_ts > t.event_ts",
    )

    # ---- 3. quarantine ----------------------------------------------------
    quarantine_frames = []
    if corrupt.count():
        quarantine_frames.append(
            corrupt.select(
                F.lit("sessions_stream").alias("_dataset"),
                F.lit("PARSE_ERROR").alias("_reject_reason"),
                F.lit("malformed JSON record in the streaming source").alias("_reject_detail"),
                F.col("_source_file"),
                F.lit(batch_id).alias("_batch_id"),
                F.current_timestamp().alias("_quarantine_ts"),
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.lit("sessions_stream"),
                        F.lit("PARSE_ERROR"),
                        F.coalesce(F.col(CORRUPT_COLUMN), F.lit("")),
                    ),
                    256,
                ).alias("_record_hash"),
                F.col(CORRUPT_COLUMN).cast("string").alias("_raw_record"),
            )
        )
    if bad_keys.count():
        quarantine_frames.append(
            bad_keys.select(
                F.lit("sessions_stream").alias("_dataset"),
                F.lit("NULL_KEY").alias("_reject_reason"),
                F.lit("null or blank business key").alias("_reject_detail"),
                F.col("_source_file"),
                F.lit(batch_id).alias("_batch_id"),
                F.current_timestamp().alias("_quarantine_ts"),
                # Content-addressed on the payload only: a byte-identical file
                # copy under a new name must not create new quarantine rows.
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.lit("sessions_stream"),
                        F.lit("NULL_KEY"),
                        F.to_json(F.struct(*[F.col(c) for c in raw_cols])),
                    ),
                    256,
                ).alias("_record_hash"),
                F.to_json(F.struct(*[F.col(c) for c in raw_cols])).alias("_raw_record"),
            )
        )
    if rejects.count():
        quarantine_frames.append(
            rejects.select(
                F.lit("sessions_stream").alias("_dataset"),
                F.col("_reject_reason"),
                F.col("_reject_detail"),
                F.col("_source_file"),
                F.lit(batch_id).alias("_batch_id"),
                F.current_timestamp().alias("_quarantine_ts"),
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.lit("sessions_stream"),
                        F.col("_reject_reason"),
                        F.coalesce(F.col("_record_hash"), F.lit("")),
                    ),
                    256,
                ).alias("_record_hash"),
                F.to_json(F.struct(*[F.col(c) for c in raw_cols])).alias("_raw_record"),
            )
        )

    if quarantine_frames:
        q = quarantine_frames[0]
        for extra in quarantine_frames[1:]:
            q = q.unionByName(extra)
        merge_upsert(spark, QUARANTINE_TABLE, q, on=["_record_hash"])

    # "newly quarantined" is a count delta, not the source row count: replaying a
    # batch must report zero, exactly like the bronze and silver counters do.
    quarantined = max(0, spark.table(QUARANTINE_TABLE).count() - quarantine_before)

    result = MicroBatchResult(
        batch_id=batch_id,
        source_files=len(file_list),
        rows_read=rows_read,
        rows_duplicate_suppressed=max(0, rows_read - bronze_inserted - quarantined),
        bronze_inserted=bronze_inserted,
        silver_inserted=silver_inserted,
        silver_updated=silver_updated,
        quarantined=quarantined,
        latency_ms=_file_age_ms(paths, settings),
        seconds=round(time.perf_counter() - started, 2),
        file_list=file_list,
    )

    # ---- record the batch (this is the replay guard) -----------------------
    _record_batch(spark, result, settings)

    typed.unpersist()
    good.unpersist()
    with_files.unpersist()
    return result


def preview_merge_counts(
    spark: SparkSession,
    target: str,
    source: DataFrame,
    keys: list[str],
    update_condition: str | None = None,
) -> tuple[int, int]:
    """Count what a MERGE *will* insert and update, without trusting its metrics.

    Delta exposes operation metrics, but their column names are engine-version
    dependent.  Measuring the effect directly keeps the reported numbers tied to
    the data: an insert is a source key absent from the target, an update is a
    matched key whose guard expression holds.
    """
    target_df = spark.table(target)
    selected = [*keys] + (["event_ts"] if "event_ts" in target_df.columns else [])
    before = target_df.select(*selected)
    inserted = source.select(*keys).join(before.select(*keys), on=keys, how="left_anti").count()
    if update_condition:
        updated = (
            source.alias("s")
            .join(before.alias("t"), on=keys, how="inner")
            .filter(F.expr(update_condition))
            .count()
        )
    else:
        updated = 0
    return int(inserted), int(updated)


def _record_batch(spark: SparkSession, result: MicroBatchResult, settings: Settings) -> None:
    ensure_table(spark, BATCH_LEDGER_TABLE, BATCH_LEDGER_SCHEMA, cdf=True, settings=settings)
    row = (
        result.batch_id,
        datetime.now(UTC).replace(tzinfo=None),
        result.source_files,
        ";".join(result.file_list),
        result.rows_read,
        result.rows_duplicate_suppressed,
        result.bronze_inserted,
        result.silver_inserted,
        result.silver_updated,
        result.quarantined,
        "yes" if result.skipped_replay else "no",
        result.latency_ms,
    )
    df = spark.createDataFrame([row], BATCH_LEDGER_SCHEMA)
    merge_upsert(spark, BATCH_LEDGER_TABLE, df, on=["batch_id"], update_condition="1 = 1")


# --------------------------------------------------------------------------
# query orchestration
# --------------------------------------------------------------------------


def build_stream(spark: SparkSession, settings: Settings) -> DataFrame:
    """The Auto-Loader-style streaming source over the landing directory."""
    raw = (
        spark.readStream.schema(RAW_SCHEMAS["sessions"])
        .option("maxFilesPerTrigger", 2)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COLUMN)
        .option("inferSchema", "false")
        .json(source_dir(settings))
    )
    # `_metadata` is a streaming-source column and is not carried into
    # foreachBatch, so project the provenance to plain columns here.
    return raw.withColumn("_source_path", F.col("_metadata.file_path")).withColumn(
        "_source_file", F.regexp_extract(F.col("_metadata.file_path"), r"([^/]+)$", 1)
    )


def run_stream(
    spark: SparkSession,
    *,
    mode: str = "once",
    settings: Settings | None = None,
    processing_time: str = "10 seconds",
    timeout_seconds: int | None = None,
) -> list[MicroBatchResult]:
    """Run the streaming ingest. ``once`` = availableNow, ``continuous`` = forever."""
    settings = settings or get_settings()
    settings.ensure_dirs()
    Path(source_dir(settings)).mkdir(parents=True, exist_ok=True)
    results: list[MicroBatchResult] = []

    def handler(batch: DataFrame, epoch_id: int) -> None:
        result = apply_micro_batch(spark, batch, settings=settings)
        results.append(result)
        print(f"  {result.summary_line()}", flush=True)

    stream = build_stream(spark, settings)
    writer = (
        stream.writeStream.foreachBatch(handler)
        .option("checkpointLocation", settings.stream_path("sessions_stream"))
        .queryName("dlg_sessions_stream")
    )
    query = (
        writer.trigger(availableNow=True).start()
        if mode == "once"
        else writer.trigger(processingTime=processing_time).start()
    )

    if mode == "once":
        query.awaitTermination()
    else:
        try:
            query.awaitTermination(timeout_seconds or 0 or None)
        except KeyboardInterrupt:  # pragma: no cover - interactive stop
            query.stop()
    return results


def replay_last_batch(spark: SparkSession, settings: Settings | None = None) -> MicroBatchResult:
    """Re-deliver the most recent micro-batch to prove the replay guard.

    The batch's source files are re-read and pushed through the *same*
    ``foreachBatch`` handler. Because the batch id is derived from the file
    contents, the handler recognises it as already applied and returns without
    touching bronze, silver or quarantine.
    """
    settings = settings or get_settings()
    batches = stream_batches(spark)
    if not batches:
        raise RuntimeError("no recorded micro-batches to replay")
    latest = batches[0]
    files = [f for f in (latest["source_file_list"] or "").split(";") if f]
    if not files:
        raise RuntimeError(f"batch {latest['batch_id']} has no recorded source files")

    directory = source_dir(settings)
    batch = (
        spark.read.schema(RAW_SCHEMAS["sessions"])
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COLUMN)
        .json(directory)
        .filter(F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1).isin(*files))
    )
    return apply_micro_batch(spark, batch, settings=settings)


def print_report(results: list[MicroBatchResult]) -> None:
    from tabulate import tabulate

    rows = [
        [
            r.batch_id,
            "yes" if r.skipped_replay else "no",
            r.source_files,
            f"{r.rows_read:,}",
            f"{r.bronze_inserted:,}",
            f"{r.silver_inserted:,}",
            f"{r.silver_updated:,}",
            f"{r.rows_duplicate_suppressed:,}",
            f"{r.quarantined:,}",
            r.latency_ms,
            f"{r.seconds:.2f}",
        ]
        for r in results
    ]
    print(
        tabulate(
            rows,
            headers=[
                "batch_id",
                "replay_skipped",
                "files",
                "rows_read",
                "bronze_+",
                "silver_+",
                "silver_~",
                "dups_suppressed",
                "quarantined",
                "latency_ms",
                "secs",
            ],
            tablefmt="github",
        )
    )
