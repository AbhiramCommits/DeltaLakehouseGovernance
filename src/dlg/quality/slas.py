"""SLA evaluation against the catalog's declared service levels.

The catalog declares three knobs per table:

* ``max_staleness_minutes`` -- how old the newest data may be,
* ``min_daily_rows``        -- the table must average at least this many rows/day,
* ``max_null_rate``         -- the highest NULL rate tolerated on any column.

Staleness is measured against the lake's own clock:

* tables that carry an ingest timestamp (``_ingest_ts`` / ``_quarantine_ts``) are
  measured against wall-clock time -- "how long since data arrived?";
* derived tables are measured against the newest business date in the lake --
  "is this mart still in sync with its source?" -- which is the check that
  actually catches a broken refresh.

Results are written to ``ops.sla_status``.
"""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from dlg.config import Settings, get_settings
from dlg.governance.catalog import Catalog, load_catalog
from dlg.lakehouse import ensure_table, new_batch_id, overwrite_delta

__all__ = ["SlaResult", "SlaRun", "run_sla_checks", "SLA_STATUS_TABLE", "SLA_STATUS_SCHEMA"]

SLA_STATUS_TABLE = "ops.sla_status"

SLA_STATUS_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("ts", TimestampType(), False),
        StructField("table", StringType(), False),
        StructField("sla_name", StringType(), False),
        StructField("threshold", DoubleType(), False),
        StructField("measured", DoubleType(), False),
        StructField("unit", StringType(), False),
        StructField("status", StringType(), False),
        StructField("detail", StringType(), True),
    ]
)

INGEST_TS_COLUMNS = ("_ingest_ts", "_quarantine_ts")
DATE_COLUMNS = ("event_date", "cohort_date")
AUDIT_PREFIX = "_"


@dataclass
class SlaResult:
    table: str
    sla_name: str
    threshold: float
    measured: float
    unit: str
    status: str
    detail: str = ""

    @property
    def breached(self) -> bool:
        return self.status == "BREACH"

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass
class SlaRun:
    run_id: str
    results: list[SlaResult] = field(default_factory=list)
    seconds: float = 0.0

    @property
    def breaches(self) -> list[SlaResult]:
        return [r for r in self.results if r.breached]

    @property
    def passed(self) -> list[SlaResult]:
        return [r for r in self.results if r.status == "OK"]

    @property
    def not_applicable(self) -> list[SlaResult]:
        return [r for r in self.results if r.status == "N/A"]

    def summary(self) -> dict:
        return {
            "run_id": self.run_id,
            "checks": len(self.results),
            "ok": len(self.passed),
            "breached": len(self.breaches),
            "not_applicable": len(self.not_applicable),
            "seconds": self.seconds,
        }


def _lake_business_clock(spark: SparkSession):
    """Newest business date in the lake (used as the reference for derived tables)."""
    row = spark.sql("SELECT max(event_date) AS d FROM silver.sessions").collect()[0]
    return row["d"]


def _staleness(spark: SparkSession, table: str, business_clock) -> tuple[float, str]:
    """Return ``(age_minutes, detail)`` for the table's newest data."""
    df = spark.table(table)
    columns = set(df.columns)

    for candidate in INGEST_TS_COLUMNS:
        if candidate in columns:
            newest = df.agg(F.max(F.col(candidate)).alias("m")).collect()[0]["m"]
            if newest is None:
                return float("inf"), f"{candidate} is entirely NULL"
            if newest.tzinfo is None:
                newest = newest.replace(tzinfo=UTC)
            age = (datetime.now(UTC) - newest).total_seconds() / 60.0
            return age, f"newest {candidate} is {age:.1f} min old (wall clock)"

    for candidate in DATE_COLUMNS:
        if candidate in columns:
            newest = df.agg(F.max(F.col(candidate)).alias("m")).collect()[0]["m"]
            if newest is None:
                return float("inf"), f"{candidate} is entirely NULL"
            age = (business_clock - newest).days * 24 * 60
            return float(age), (
                f"newest {candidate} is {business_clock - newest} behind the lake business clock"
            )

    return 0.0, "no temporal column; staleness not applicable"


def run_sla_checks(
    spark: SparkSession,
    *,
    catalog: Catalog | None = None,
    settings: Settings | None = None,
    run_id: str | None = None,
    persist: bool = True,
) -> SlaRun:
    settings = settings or get_settings()
    catalog = catalog or load_catalog(settings=settings)
    run_id = run_id or new_batch_id("sla")
    business_clock = _lake_business_clock(spark)

    started = time.perf_counter()
    results: list[SlaResult] = []

    for name, table in catalog.tables.items():
        try:
            df = spark.table(name)
        except Exception:
            continue

        sla = table.sla

        # ---- staleness ---------------------------------------------------
        age_minutes, detail = _staleness(spark, name, business_clock)
        results.append(
            SlaResult(
                table=name,
                sla_name="max_staleness_minutes",
                threshold=float(sla.max_staleness_minutes),
                measured=round(age_minutes, 2),
                unit="minutes",
                status="OK" if age_minutes <= sla.max_staleness_minutes else "BREACH",
                detail=detail,
            )
        )

        # ---- volume ------------------------------------------------------
        total = df.count()
        date_column = next((c for c in DATE_COLUMNS if c in df.columns), None)
        if date_column:
            days = df.select(F.col(date_column)).distinct().count()
        else:
            days = 1
        per_day = total / days if days else float(total)
        results.append(
            SlaResult(
                table=name,
                sla_name="min_daily_rows",
                threshold=float(sla.min_daily_rows),
                measured=round(per_day, 2),
                unit="rows/day",
                status="OK" if per_day >= sla.min_daily_rows else "BREACH",
                detail=f"{total:,} rows across {days} day(s)",
            )
        )

        # ---- null rate (max across non-audit columns) ----------------------
        measured_columns = [c for c in df.columns if not c.startswith(AUDIT_PREFIX)]
        if not total:
            results.append(
                SlaResult(
                    table=name,
                    sla_name="max_null_rate",
                    threshold=float(sla.max_null_rate),
                    measured=0.0,
                    unit="rate",
                    status="OK",
                    detail="table is empty",
                )
            )
        elif not measured_columns:
            # Audit-only tables (quarantine, ops) have no business column whose
            # NULL rate is meaningful, so this is reported as N/A rather than as
            # a spurious 100% breach on `_source_file`.
            results.append(
                SlaResult(
                    table=name,
                    sla_name="max_null_rate",
                    threshold=float(sla.max_null_rate),
                    measured=0.0,
                    unit="rate",
                    status="N/A",
                    detail="table has only audit columns; null-rate check not applicable",
                )
            )
        else:
            agg = df.agg(
                *[
                    (F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)) / F.lit(total)).alias(c)
                    for c in measured_columns
                ]
            ).collect()[0]
            rates = {c: float(agg[c] or 0.0) for c in measured_columns}
            worst_column = max(rates, key=lambda c: rates[c])
            worst = rates[worst_column]
            results.append(
                SlaResult(
                    table=name,
                    sla_name="max_null_rate",
                    threshold=float(sla.max_null_rate),
                    measured=round(worst, 6),
                    unit="rate",
                    status="OK" if worst <= sla.max_null_rate else "BREACH",
                    detail=f"worst column is {worst_column or 'n/a'}",
                )
            )

    run = SlaRun(run_id=run_id, results=results, seconds=round(time.perf_counter() - started, 2))

    if persist:
        ensure_table(spark, SLA_STATUS_TABLE, SLA_STATUS_SCHEMA, cdf=True, settings=settings)
        ts = datetime.now(UTC).replace(tzinfo=None)
        rows = [
            (
                run_id,
                ts,
                r.table,
                r.sla_name,
                float(r.threshold),
                float(r.measured) if r.measured != float("inf") else -1.0,
                r.unit,
                r.status,
                r.detail,
            )
            for r in run.results
        ]
        overwrite_delta(spark, SLA_STATUS_TABLE, spark.createDataFrame(rows, SLA_STATUS_SCHEMA))
    return run


def print_report(run: SlaRun) -> None:
    from tabulate import tabulate

    rows = [
        [r.table, r.sla_name, f"{r.threshold:g}", f"{r.measured:g}", r.unit, r.status, r.detail]
        for r in run.results
    ]
    print(
        tabulate(
            rows,
            headers=["table", "sla", "threshold", "measured", "unit", "status", "detail"],
            tablefmt="github",
        )
    )
    s = run.summary()
    print(
        f"\nSLA run {s['run_id']}: {s['checks']} checks, {s['ok']} OK, "
        f"{s['breached']} breached, {s['not_applicable']} N/A, {s['seconds']}s"
    )
    for b in run.breaches:
        print(
            f"  BREACH {b.table}.{b.sla_name}: measured {b.measured:g} {b.unit} "
            f"vs threshold {b.threshold:g} ({b.detail})"
        )
