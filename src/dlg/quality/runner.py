"""Execute the expectation suites and persist the results.

Results land in ``ops.dq_results`` (one row per expectation per run) and are
returned as a :class:`QualityRun` so the CLI, the alerting layer and the results
report all work from the same measured numbers.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from dlg.config import Settings, get_settings
from dlg.lakehouse import ensure_table, new_batch_id, overwrite_delta
from dlg.quality.expectations import Expectation, ExpectationResult, evaluate, load_suite_file

__all__ = ["QualityRun", "run_quality", "DQ_RESULTS_TABLE", "DQ_RESULTS_SCHEMA"]

DQ_RESULTS_TABLE = "ops.dq_results"

DQ_RESULTS_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("ts", TimestampType(), False),
        StructField("table", StringType(), False),
        StructField("expectation", StringType(), False),
        StructField("kind", StringType(), False),
        StructField("column", StringType(), True),
        StructField("severity", StringType(), False),
        StructField("rows_checked", LongType(), False),
        StructField("rows_failed", LongType(), False),
        StructField("fail_rate", DoubleType(), False),
        StructField("threshold", DoubleType(), False),
        StructField("status", StringType(), False),
        StructField("detail", StringType(), True),
        StructField("duration_ms", IntegerType(), False),
    ]
)


def default_suites_path(settings: Settings | None = None) -> Path:
    settings = settings or get_settings()
    del settings
    return Path(__file__).with_name("suites.yml")


@dataclass
class QualityRun:
    run_id: str
    results: list[ExpectationResult] = field(default_factory=list)
    seconds: float = 0.0
    suites_file: str = ""

    # ---- summaries -----------------------------------------------------
    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def passed(self) -> list[ExpectationResult]:
        return [r for r in self.results if r.status == "PASS"]

    @property
    def failed(self) -> list[ExpectationResult]:
        return [r for r in self.results if r.status == "FAIL"]

    @property
    def blocking_failures(self) -> list[ExpectationResult]:
        return [r for r in self.results if r.is_blocking_failure]

    @property
    def warn_failures(self) -> list[ExpectationResult]:
        return [r for r in self.results if r.status == "FAIL" and r.severity == "WARN"]

    @property
    def pass_rate(self) -> float:
        return (len(self.passed) / self.total) if self.total else 0.0

    def summary(self) -> dict:
        return {
            "run_id": self.run_id,
            "total": self.total,
            "pass": len(self.passed),
            "fail": len(self.failed),
            "blocking_failures": len(self.blocking_failures),
            "warn_failures": len(self.warn_failures),
            "pass_rate": round(self.pass_rate, 4),
            "seconds": self.seconds,
            "suites_file": self.suites_file,
        }


def _write_results(spark: SparkSession, run: QualityRun, settings: Settings) -> None:
    ensure_table(spark, DQ_RESULTS_TABLE, DQ_RESULTS_SCHEMA, cdf=True, settings=settings)
    ts = datetime.now(UTC).replace(tzinfo=None)
    rows = [
        (
            r.run_id,
            ts,
            r.table,
            r.expectation,
            r.kind,
            r.column,
            r.severity,
            r.rows_checked,
            r.rows_failed,
            r.fail_rate,
            r.threshold,
            r.status,
            r.detail,
            r.duration_ms,
        )
        for r in run.results
    ]
    df = spark.createDataFrame(rows, schema=DQ_RESULTS_SCHEMA)
    overwrite_delta(spark, DQ_RESULTS_TABLE, df)


def run_quality(
    spark: SparkSession,
    *,
    suites_path: str | Path | None = None,
    settings: Settings | None = None,
    run_id: str | None = None,
) -> QualityRun:
    """Evaluate every expectation in ``suites.yml`` and persist the results."""
    settings = settings or get_settings()
    path = Path(suites_path) if suites_path else default_suites_path(settings)
    expectations, _meta = load_suite_file(str(path))
    run_id = run_id or new_batch_id("dq")

    started = time.perf_counter()
    results: list[ExpectationResult] = []
    for exp in expectations:
        results.append(evaluate(spark, exp, run_id))
    elapsed = round(time.perf_counter() - started, 2)

    run = QualityRun(run_id=run_id, results=results, seconds=elapsed, suites_file=str(path))
    _write_results(spark, run, settings)
    return run


def print_report(run: QualityRun) -> None:
    from tabulate import tabulate

    rows = []
    for r in run.results:
        rows.append(
            [
                r.table,
                r.expectation,
                r.severity,
                r.status,
                f"{r.rows_checked:,}",
                f"{r.rows_failed:,}",
                f"{r.fail_rate:.6f}",
                f"{r.threshold:.6f}",
                f"{r.duration_ms}",
            ]
        )
    print(
        tabulate(
            rows,
            headers=[
                "table",
                "expectation",
                "severity",
                "status",
                "checked",
                "failed",
                "fail_rate",
                "threshold",
                "ms",
            ],
            tablefmt="github",
        )
    )
    s = run.summary()
    print(
        f"\nDQ run {s['run_id']}: {s['total']} expectations, {s['pass']} pass, "
        f"{s['fail']} fail ({s['blocking_failures']} BLOCKING, {s['warn_failures']} WARN), "
        f"pass rate {100 * s['pass_rate']:.2f}%, {s['seconds']}s"
    )
    for r in run.failed:
        print(f"  FAIL[{r.severity}] {r.table}.{r.expectation}: {r.detail}")


def results_frame(spark: SparkSession):
    """The persisted results table (handy for tests)."""
    return spark.table(DQ_RESULTS_TABLE)


__all__ += ["results_frame"]


def as_dict_list(items: list[Expectation]) -> list[dict]:  # pragma: no cover - helper
    return [i.as_dict() for i in items]
