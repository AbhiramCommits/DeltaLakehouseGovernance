"""Delta time-travel / rollback tooling.

Delta's transaction log is the safety net that makes a lakehouse operable:

* ``history`` shows every commit with its operation and metrics;
* ``demo-rollback`` deliberately runs a *destructive* MERGE (the classic "bad
  backfill duplicated the table" incident), measures the damage, then uses
  ``RESTORE TABLE ... TO VERSION AS OF`` to put the table back, asserting the
  row count matches the pre-incident snapshot.
"""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass

from pyspark.sql import SparkSession

from dlg.config import get_settings
from dlg.lakehouse import delta_history, delta_version, merge_upsert, row_count

__all__ = ["print_history", "demo_rollback", "RollbackReport", "DEFAULT_ROLLBACK_TABLE"]

DEFAULT_ROLLBACK_TABLE = "silver.purchases"


@dataclass
class RollbackReport:
    table: str
    version_before: int
    count_before: int
    version_corrupt: int
    count_corrupt: int
    rows_added: int
    version_after: int
    count_after: int
    restored_equal: bool
    seconds: float

    def as_dict(self) -> dict:
        return asdict(self)


def print_history(spark: SparkSession, table: str, limit: int = 20) -> list[dict]:
    """Print version, operation and key metrics for a Delta table."""
    from tabulate import tabulate

    hist = delta_history(spark, table, limit=limit)
    rows = []
    for entry in hist:
        metrics = entry.get("operationMetrics") or {}
        rows.append(
            [
                entry.get("version"),
                str(entry.get("timestamp")),
                entry.get("operation"),
                metrics.get("numOutputRows", metrics.get("numFiles", "")),
                metrics.get("numAddedFiles", ""),
                metrics.get("numRemovedFiles", ""),
            ]
        )
    print(f"# Delta history for {table} (latest {min(limit, len(hist))} commits)")
    print(
        tabulate(
            rows,
            headers=["version", "timestamp", "operation", "rows", "files+", "files-"],
            tablefmt="github",
        )
    )
    return hist


def demo_rollback(spark: SparkSession, table: str = DEFAULT_ROLLBACK_TABLE) -> RollbackReport:
    """Corrupt a table with a bad MERGE, measure it, then RESTORE and assert."""
    started = time.perf_counter()

    version_before = delta_version(spark, table)
    count_before = row_count(spark, table)
    print(f"[rollback] {table}: version={version_before} rows={count_before:,}")

    # --- the incident: a backfill that re-inserts every row under a new key --
    bad_source = spark.sql(
        f"""
        SELECT
          CONCAT('BAD_', purchase_id) AS purchase_id,
          session_id, player_id, sku, item_name, category,
          price_usd, currency, quantity, purchase_ts, event_ts, event_date,
          payment_method, _record_hash, _ingest_ts, _batch_id
        FROM {table}
        """
    )
    merge_upsert(spark, table, bad_source, on=["purchase_id"])

    version_corrupt = delta_version(spark, table)
    count_corrupt = row_count(spark, table)
    rows_added = count_corrupt - count_before
    print(
        f"[rollback] after bad MERGE: version={version_corrupt} "
        f"rows={count_corrupt:,} (+{rows_added:,})"
    )
    assert count_corrupt != count_before, "the destructive MERGE did not change the data"

    # --- recovery: Delta RESTORE to the pre-incident snapshot -----------------
    spark.sql(f"RESTORE TABLE {table} TO VERSION AS OF {version_before}")
    version_after = delta_version(spark, table)
    count_after = row_count(spark, table)
    print(
        f"[rollback] after RESTORE TO VERSION AS OF {version_before}: "
        f"version={version_after} rows={count_after:,}"
    )

    restored_equal = count_after == count_before
    assert restored_equal, f"RESTORE did not recover the row count: {count_after} != {count_before}"
    print(
        f"[rollback] OK — restored row count equals snapshot ({count_after:,} == {count_before:,})"
    )

    return RollbackReport(
        table=table,
        version_before=version_before,
        count_before=count_before,
        version_corrupt=version_corrupt,
        count_corrupt=count_corrupt,
        rows_added=rows_added,
        version_after=version_after,
        count_after=count_after,
        restored_equal=restored_equal,
        seconds=round(time.perf_counter() - started, 2),
    )


def main(argv: list[str] | None = None) -> None:  # pragma: no cover - CLI shim
    from dlg.spark import get_spark

    argv = argv or []
    spark = get_spark("dlg-timetravel")
    settings = get_settings()
    if argv and argv[0] == "history":
        print_history(spark, argv[1] if len(argv) > 1 else "silver.sessions")
    else:
        demo_rollback(spark, argv[0] if argv else DEFAULT_ROLLBACK_TABLE)
    del settings
