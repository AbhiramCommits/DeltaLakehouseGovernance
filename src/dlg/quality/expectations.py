"""A small, in-repo declarative expectation framework.

No external dependency (no Great Expectations): each expectation is a declarative
record that evaluates to a single :class:`ExpectationResult` carrying the counts
that matter for gating and for the results report:

``rows_checked``, ``rows_failed``, ``fail_rate`` and pass/fail against a threshold.

Supported kinds:

===============  ==========================================================
``not_null``              the column has no NULL values
``unique``                the column set is a unique key
``accepted_values``       the column only contains values from a set
``range``                 numeric values fall inside ``[min, max]``
``regex``                 string values match a regular expression
``referential_integrity`` values exist in a reference table.column
``row_count_between``     the table has between ``min_rows`` and ``max_rows`` rows
``row_count_ratio``       the table's row count is a sane fraction of a reference table
``freshness_within``      the newest value in the column is within N minutes
===============  ==========================================================
"""

from __future__ import annotations

import re
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from dlg.lakehouse import TableRef  # noqa: F401  (re-exported for callers)

__all__ = ["Expectation", "ExpectationResult", "KINDS", "evaluate", "load_suite_file"]

KINDS = (
    "not_null",
    "unique",
    "accepted_values",
    "range",
    "regex",
    "referential_integrity",
    "row_count_between",
    "row_count_ratio",
    "freshness_within",
)

SEVERITIES = ("BLOCKING", "WARN", "INFO")

#: Reference instant for freshness checks. Set by the runner so every
#: expectation in a run agrees on "now" (and so tests can pin it).
_NOW_OVERRIDE: Any = None


def set_now(value: Any) -> None:
    """Pin the freshness reference time (used by tests)."""
    global _NOW_OVERRIDE
    _NOW_OVERRIDE = value


@dataclass
class Expectation:
    """A single declarative expectation."""

    kind: str
    table: str
    name: str = ""
    severity: str = "BLOCKING"
    column: str | None = None
    columns: list[str] = field(default_factory=list)
    values: list[Any] = field(default_factory=list)
    min_value: float | None = None
    max_value: float | None = None
    pattern: str | None = None
    ref_table: str | None = None
    ref_column: str | None = None
    min_rows: int | None = None
    max_rows: int | None = None
    min_ratio: float | None = None
    max_ratio: float | None = None
    max_staleness_minutes: float | None = None
    max_fail_rate: float = 0.0
    description: str = ""

    def __post_init__(self) -> None:
        if self.kind not in KINDS:
            raise ValueError(f"unknown expectation kind {self.kind!r}; expected one of {KINDS}")
        severity = self.severity.upper()
        if severity not in SEVERITIES:
            raise ValueError(f"unknown severity {severity!r}; expected one of {SEVERITIES}")
        self.severity = severity
        if not self.name:
            target = self.column or ",".join(self.columns) or self.table
            self.name = f"{self.kind}({target})"
        if self.column and not self.columns:
            self.columns = [self.column]

    @classmethod
    def from_dict(cls, table: str, data: dict) -> Expectation:
        known = {
            "kind",
            "table",
            "name",
            "severity",
            "column",
            "columns",
            "values",
            "min_value",
            "max_value",
            "pattern",
            "ref_table",
            "ref_column",
            "min_rows",
            "max_rows",
            "max_staleness_minutes",
            "max_fail_rate",
            "description",
        }
        payload = {k: v for k, v in data.items() if k in known}
        payload.setdefault("table", table)
        return cls(**payload)

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass
class ExpectationResult:
    run_id: str
    table: str
    expectation: str
    kind: str
    column: str | None
    severity: str
    rows_checked: int
    rows_failed: int
    fail_rate: float
    threshold: float
    status: str
    detail: str = ""
    duration_ms: int = 0

    @property
    def passed(self) -> bool:
        return self.status == "PASS"

    @property
    def is_blocking_failure(self) -> bool:
        return self.status == "FAIL" and self.severity == "BLOCKING"

    def as_dict(self) -> dict:
        return asdict(self)


# --------------------------------------------------------------------------
# evaluators
# --------------------------------------------------------------------------


def _result(
    exp: Expectation,
    run_id: str,
    rows_checked: int,
    rows_failed: int,
    passed: bool,
    threshold: float,
    detail: str,
    started: float,
) -> ExpectationResult:
    fail_rate = (rows_failed / rows_checked) if rows_checked else 0.0
    return ExpectationResult(
        run_id=run_id,
        table=exp.table,
        expectation=exp.name,
        kind=exp.kind,
        column=exp.column,
        severity=exp.severity,
        rows_checked=int(rows_checked),
        rows_failed=int(rows_failed),
        # PySpark's DoubleType verifier is strict about ints, so cast explicitly.
        fail_rate=float(round(fail_rate, 6)),
        threshold=float(round(float(threshold), 6)),
        status="PASS" if passed else "FAIL",
        detail=detail,
        duration_ms=int((time.perf_counter() - started) * 1000),
    )


def _not_null(spark: SparkSession, exp: Expectation, run_id: str) -> ExpectationResult:
    started = time.perf_counter()
    column = exp.column or exp.columns[0]
    df = spark.table(exp.table)
    total = df.count()
    failed = df.filter(F.col(column).isNull()).count()
    threshold = exp.max_fail_rate
    return _result(
        exp,
        run_id,
        total,
        failed,
        (failed / total if total else 0.0) <= threshold,
        threshold,
        f"{column} has {failed} NULL value(s) of {total}",
        started,
    )


def _unique(spark: SparkSession, exp: Expectation, run_id: str) -> ExpectationResult:
    started = time.perf_counter()
    columns = exp.columns or ([exp.column] if exp.column else [])
    df = spark.table(exp.table)
    total = df.count()
    distinct = df.select(*columns).distinct().count()
    failed = max(0, total - distinct)
    threshold = exp.max_fail_rate
    return _result(
        exp,
        run_id,
        total,
        failed,
        (failed / total if total else 0.0) <= threshold,
        threshold,
        f"{','.join(columns)}: {distinct} distinct of {total} rows ({failed} duplicate row(s))",
        started,
    )


def _accepted_values(spark: SparkSession, exp: Expectation, run_id: str) -> ExpectationResult:
    started = time.perf_counter()
    column = exp.column or exp.columns[0]
    df = spark.table(exp.table)
    total = df.count()
    present = df.filter(F.col(column).isNotNull())
    checked = present.count()
    failed = present.filter(~F.col(column).isin(*exp.values)).count()
    threshold = exp.max_fail_rate
    return _result(
        exp,
        run_id,
        checked,
        failed,
        (failed / checked if checked else 0.0) <= threshold,
        threshold,
        f"{column}: {failed} row(s) outside {list(exp.values)} (of {checked} non-null; "
        f"{total - checked} NULL excluded)",
        started,
    )


def _range(spark: SparkSession, exp: Expectation, run_id: str) -> ExpectationResult:
    started = time.perf_counter()
    column = exp.column or exp.columns[0]
    df = spark.table(exp.table).filter(F.col(column).isNotNull())
    checked = df.count()
    condition = F.lit(True)
    if exp.min_value is not None:
        condition = condition & (F.col(column) >= F.lit(exp.min_value))
    if exp.max_value is not None:
        condition = condition & (F.col(column) <= F.lit(exp.max_value))
    failed = df.filter(~condition).count()
    threshold = exp.max_fail_rate
    return _result(
        exp,
        run_id,
        checked,
        failed,
        (failed / checked if checked else 0.0) <= threshold,
        threshold,
        f"{column}: {failed} value(s) outside [{exp.min_value}, {exp.max_value}] of {checked}",
        started,
    )


def _regex(spark: SparkSession, exp: Expectation, run_id: str) -> ExpectationResult:
    started = time.perf_counter()
    column = exp.column or exp.columns[0]
    pattern = exp.pattern or ".*"
    df = spark.table(exp.table).filter(F.col(column).isNotNull())
    checked = df.count()
    failed = df.filter(~F.col(column).rlike(pattern)).count()
    threshold = exp.max_fail_rate
    re.compile(pattern)  # fail fast on an invalid pattern, even if no rows matched
    return _result(
        exp,
        run_id,
        checked,
        failed,
        (failed / checked if checked else 0.0) <= threshold,
        threshold,
        f"{column}: {failed} value(s) do not match /{pattern}/ of {checked} non-null",
        started,
    )


def _referential_integrity(spark: SparkSession, exp: Expectation, run_id: str) -> ExpectationResult:
    started = time.perf_counter()
    column = exp.column or exp.columns[0]
    ref_table = exp.ref_table or ""
    ref_column = exp.ref_column or column
    df = spark.table(exp.table).filter(F.col(column).isNotNull())
    checked = df.count()
    reference = spark.table(ref_table).select(F.col(ref_column).alias("_ref")).distinct()
    failed = (
        df.select(F.col(column).alias("_val"))
        .join(reference, F.col("_val") == F.col("_ref"), how="left_anti")
        .count()
    )
    threshold = exp.max_fail_rate
    return _result(
        exp,
        run_id,
        checked,
        failed,
        (failed / checked if checked else 0.0) <= threshold,
        threshold,
        f"{column}: {failed} value(s) missing from {ref_table}.{ref_column} of {checked}",
        started,
    )


def _row_count_between(spark: SparkSession, exp: Expectation, run_id: str) -> ExpectationResult:
    started = time.perf_counter()
    total = spark.table(exp.table).count()
    low = exp.min_rows if exp.min_rows is not None else 0
    high = exp.max_rows if exp.max_rows is not None else 10**12
    passed = low <= total <= high
    return _result(
        exp,
        run_id,
        total,
        0 if passed else total,
        passed,
        0.0,
        f"{exp.table} has {total} rows; expected between {low} and {high}",
        started,
    )


def _row_count_ratio(spark: SparkSession, exp: Expectation, run_id: str) -> ExpectationResult:
    """Scale-free volume guard: rows(table) / rows(ref_table) inside a band.

    Absolute row-count guards are tuned for one dataset size; a ratio guard
    catches the failure that matters (a silent drop or duplication) at any scale,
    which is what lets the same suites.yml gate CI's reduced run.
    """
    started = time.perf_counter()
    total = spark.table(exp.table).count()
    reference = spark.table(exp.ref_table or "").count()
    ratio = (total / reference) if reference else 0.0
    low = exp.min_ratio if exp.min_ratio is not None else 0.0
    high = exp.max_ratio if exp.max_ratio is not None else float("inf")
    passed = low <= ratio <= high
    return _result(
        exp,
        run_id,
        total,
        0 if passed else total,
        passed,
        0.0,
        f"{exp.table} has {total} rows, {ratio:.4f}x {exp.ref_table} "
        f"(expected between {low} and {high})",
        started,
    )


def _freshness_within(spark: SparkSession, exp: Expectation, run_id: str) -> ExpectationResult:
    started = time.perf_counter()
    column = exp.column or "event_ts"
    limit = exp.max_staleness_minutes or 1440
    newest = spark.table(exp.table).agg(F.max(F.col(column)).alias("m")).collect()[0]["m"]
    if newest is None:
        return _result(exp, run_id, 0, 1, False, limit, f"{column} is entirely NULL", started)
    now = _NOW_OVERRIDE or datetime.now(UTC)
    if newest.tzinfo is None:
        newest = newest.replace(tzinfo=UTC)
    age_minutes = (now - newest).total_seconds() / 60.0
    passed = age_minutes <= limit
    return _result(
        exp,
        run_id,
        1,
        0 if passed else 1,
        passed,
        limit,
        f"{column}: newest value is {age_minutes:.1f} minutes old (limit {limit})",
        started,
    )


_EVALUATORS = {
    "not_null": _not_null,
    "unique": _unique,
    "accepted_values": _accepted_values,
    "range": _range,
    "regex": _regex,
    "referential_integrity": _referential_integrity,
    "row_count_between": _row_count_between,
    "row_count_ratio": _row_count_ratio,
    "freshness_within": _freshness_within,
}


def evaluate(spark: SparkSession, exp: Expectation, run_id: str = "adhoc") -> ExpectationResult:
    """Evaluate one expectation and return its measured result."""
    return _EVALUATORS[exp.kind](spark, exp, run_id)


def load_suite_file(path: str) -> tuple[list[Expectation], dict]:
    """Load suites.yml into ``(expectations, metadata)``."""
    from pathlib import Path

    import yaml

    raw = yaml.safe_load(Path(path).read_text())
    expectations: list[Expectation] = []
    for suite in raw.get("suites") or []:
        table = suite["table"]
        for item in suite.get("expectations") or []:
            expectations.append(Expectation.from_dict(table, item))
    metadata = {
        "version": raw.get("version", 1),
        "suites": len(raw.get("suites") or []),
        "tables": sorted({s["table"] for s in raw.get("suites") or []}),
    }
    return expectations, metadata
