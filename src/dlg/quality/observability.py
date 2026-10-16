"""Observability snapshot: per-table metrics and a markdown report.

For every table in the catalog this records the row count, on-disk size, file
count, the Delta commit version/timestamp, and the NULL rate on key columns
(primary keys and the partition column).  Results are written to
``ops.table_metrics`` and rendered to ``results/observability_report.md``.
"""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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
from dlg.governance.catalog import Catalog, load_catalog
from dlg.lakehouse import ensure_table, last_commit, overwrite_delta, table_size_bytes

__all__ = ["TableMetrics", "ObservabilitySnapshot", "observe", "TABLE_METRICS_TABLE"]

TABLE_METRICS_TABLE = "ops.table_metrics"

TABLE_METRICS_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("ts", TimestampType(), False),
        StructField("table", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("domain", StringType(), False),
        StructField("owner", StringType(), False),
        StructField("classification", StringType(), False),
        StructField("rows", LongType(), False),
        StructField("size_bytes", LongType(), False),
        StructField("num_files", IntegerType(), False),
        StructField("delta_version", LongType(), False),
        StructField("last_commit_ts", TimestampType(), True),
        StructField("key_column", StringType(), True),
        StructField("key_null_rate", DoubleType(), False),
        StructField("pii_columns", IntegerType(), False),
    ]
)


@dataclass
class TableMetrics:
    table: str
    layer: str
    domain: str
    owner: str
    classification: str
    rows: int
    size_bytes: int
    num_files: int
    delta_version: int
    last_commit_ts: str | None
    key_column: str | None
    key_null_rate: float
    pii_columns: int

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass
class ObservabilitySnapshot:
    run_id: str
    tables: list[TableMetrics] = field(default_factory=list)
    seconds: float = 0.0

    def summary(self) -> dict:
        return {
            "run_id": self.run_id,
            "tables": len(self.tables),
            "total_rows": sum(t.rows for t in self.tables),
            "total_bytes": sum(t.size_bytes for t in self.tables),
            "total_files": sum(t.num_files for t in self.tables),
            "seconds": self.seconds,
        }


def _key_columns(catalog_table) -> list[str]:
    keys = [
        name
        for name, column in catalog_table.columns.items()
        if "pk" in [k.lower() for k in column.keys]
    ]
    for candidate in ("event_date", "cohort_date"):
        if candidate in catalog_table.columns and candidate not in keys:
            keys.append(candidate)
    return keys or list(catalog_table.columns)[:1]


def observe(
    spark: SparkSession,
    *,
    catalog: Catalog | None = None,
    settings: Settings | None = None,
    run_id: str = "observe",
    persist: bool = True,
) -> ObservabilitySnapshot:
    settings = settings or get_settings()
    catalog = catalog or load_catalog(settings=settings)
    started = time.perf_counter()
    rows: list[TableMetrics] = []

    for name, table in catalog.tables.items():
        try:
            df = spark.table(name)
        except Exception:
            continue
        count = df.count()
        detail = spark.sql(f"DESCRIBE DETAIL {name}").collect()[0]
        version, commit_ts = last_commit(spark, name)

        key_column = None
        key_null_rate = 0.0
        for candidate in _key_columns(table):
            if candidate not in df.columns:
                continue
            nulls = df.filter(F.col(candidate).isNull()).count()
            rate = (nulls / count) if count else 0.0
            if rate > key_null_rate or key_column is None:
                key_column, key_null_rate = candidate, rate
            if rate > 0:
                break

        rows.append(
            TableMetrics(
                table=name,
                layer=table.layer,
                domain=table.domain,
                owner=table.owner,
                classification=table.classification,
                rows=int(count),
                size_bytes=int(table_size_bytes(spark, name)),
                num_files=int(detail["numFiles"] or 0),
                delta_version=int(version),
                last_commit_ts=str(commit_ts) if commit_ts is not None else None,
                key_column=key_column,
                key_null_rate=round(key_null_rate, 6),
                pii_columns=len(table.pii_columns),
            )
        )

    snapshot = ObservabilitySnapshot(
        run_id=run_id, tables=rows, seconds=round(time.perf_counter() - started, 2)
    )

    if persist:
        ensure_table(spark, TABLE_METRICS_TABLE, TABLE_METRICS_SCHEMA, cdf=True, settings=settings)
        ts = datetime.now(UTC).replace(tzinfo=None)
        payload = [
            (
                run_id,
                ts,
                m.table,
                m.layer,
                m.domain,
                m.owner,
                m.classification,
                m.rows,
                m.size_bytes,
                m.num_files,
                m.delta_version,
                datetime.fromisoformat(m.last_commit_ts) if m.last_commit_ts else None,
                m.key_column,
                m.key_null_rate,
                m.pii_columns,
            )
            for m in rows
        ]
        overwrite_delta(
            spark, TABLE_METRICS_TABLE, spark.createDataFrame(payload, TABLE_METRICS_SCHEMA)
        )

    return snapshot


def write_report(snapshot: ObservabilitySnapshot, settings: Settings | None = None) -> str:
    """Render ``results/observability_report.md``."""
    settings = settings or get_settings()
    path = f"{settings.results_dir}/observability_report.md"
    total_rows = sum(t.rows for t in snapshot.tables)
    total_bytes = sum(t.size_bytes for t in snapshot.tables)

    lines = [
        "# Observability report",
        "",
        f"Generated {datetime.now(UTC).isoformat()} by `dlg observe` (run `{snapshot.run_id}`).",
        "",
        "## Lake summary",
        "",
        "| metric | value |",
        "| --- | ---: |",
        f"| tables | {len(snapshot.tables)} |",
        f"| total rows | {total_rows:,} |",
        f"| total size (MB) | {total_bytes / 1e6:.1f} |",
        f"| total files | {sum(t.num_files for t in snapshot.tables):,} |",
        f"| snapshot seconds | {snapshot.seconds} |",
        "",
        "## Per-table metrics",
        "",
        "| table | layer | domain | class | rows | size (MB) | files | delta v | last commit | key | key null rate | PII cols |",
        "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- | ---: | ---: |",
    ]
    for m in sorted(snapshot.tables, key=lambda t: (t.layer, t.table)):
        lines.append(
            f"| `{m.table}` | {m.layer} | {m.domain} | {m.classification} | {m.rows:,} | "
            f"{m.size_bytes / 1e6:.2f} | {m.num_files} | {m.delta_version} | "
            f"{m.last_commit_ts or '—'} | {m.key_column or '—'} | {m.key_null_rate:.4f} | "
            f"{m.pii_columns} |"
        )

    lines += [
        "",
        "## Notes",
        "",
        "* `key null rate` is the NULL rate on the first primary-key column found in the",
        "  catalog for each table (or the partition column when there is no primary key).",
        "* `delta v` is the latest Delta commit version, i.e. how many times the table has",
        "  been written; `last commit` is that commit's timestamp.",
        "* Tables whose catalog classification is `PII` still list PII columns: the masking",
        "  happens in the `secure.*` views, not by deleting data from the lake.",
    ]
    from pathlib import Path

    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text("\n".join(lines) + "\n")
    return path
