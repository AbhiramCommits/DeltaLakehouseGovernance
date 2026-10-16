"""Thin, reusable helpers over the Delta catalog.

Everything the pipelines need to talk to Delta lives here: DDL generation from an
explicit ``StructType``, idempotent MERGE upserts, row counting, Delta history and
table/column introspection.  Keeping it in one place means the bronze, silver, gold,
quality and governance layers all agree on how tables are named and created.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from dlg.config import Settings, get_settings

__all__ = [
    "TableRef",
    "new_batch_id",
    "ddl_for",
    "ensure_database",
    "ensure_table",
    "table_exists",
    "merge_upsert",
    "row_count",
    "delta_history",
    "delta_version",
    "last_commit",
    "table_size_bytes",
    "column_types",
    "sql_type",
]


class TableRef:
    """A ``layer.table`` reference that resolves to a physical path + name."""

    def __init__(self, name: str, settings: Settings | None = None) -> None:
        if "." not in name:
            raise ValueError(f"expected '<layer>.<table>', got {name!r}")
        self.name = name
        self.layer, self.table = name.split(".", 1)
        self.settings = settings or get_settings()
        self.path = self.settings.table_path(name)

    def __str__(self) -> str:  # pragma: no cover - trivial
        return self.name

    def __repr__(self) -> str:  # pragma: no cover - trivial
        return f"TableRef({self.name!r})"


def new_batch_id(prefix: str = "batch") -> str:
    """A sortable, unique-ish batch identifier (UTC, millisecond precision)."""
    return f"{prefix}_{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%f')}"


# --------------------------------------------------------------------------
# DDL
# --------------------------------------------------------------------------

_TYPE_ALIASES = {
    "integer": "INT",
    "string": "STRING",
    "timestamp": "TIMESTAMP",
    "date": "DATE",
    "double": "DOUBLE",
    "bigint": "BIGINT",
    "boolean": "BOOLEAN",
    "float": "FLOAT",
    "long": "BIGINT",
    "short": "SMALLINT",
    "byte": "TINYINT",
}


def sql_type(data_type: object) -> str:
    """Map a PySpark data type to a Spark SQL type keyword."""
    simple = data_type.simpleString()  # type: ignore[attr-defined]
    return _TYPE_ALIASES.get(simple, simple.upper())


def ddl_for(
    table: str,
    schema: StructType,
    location: str,
    partition_by: Iterable[str] | None = None,
    properties: dict[str, str] | None = None,
) -> str:
    """Build a ``CREATE TABLE`` statement from an explicit schema."""
    cols = []
    for field in schema.fields:
        nullable = "" if field.nullable else " NOT NULL"
        cols.append(f"  `{field.name}` {sql_type(field.dataType)}{nullable}")
    body = ",\n".join(cols)
    stmt = f"CREATE TABLE IF NOT EXISTS {table} (\n{body}\n) USING DELTA\nLOCATION '{location}'"
    if partition_by:
        parts = ", ".join(f"`{c}`" for c in partition_by)
        stmt += f"\nPARTITIONED BY ({parts})"
    props = dict(properties or {})
    if props:
        rendered = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())
        stmt += f"\nTBLPROPERTIES ({rendered})"
    return stmt


def ensure_database(spark: SparkSession, layer: str) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {layer}")


def ensure_table(
    spark: SparkSession,
    name: str,
    schema: StructType,
    *,
    partition_by: Iterable[str] | None = None,
    cdf: bool = True,
    extra_properties: dict[str, str] | None = None,
    settings: Settings | None = None,
) -> TableRef:
    """Create ``name`` if absent, with schema enforcement and optional CDF."""
    ref = TableRef(name, settings)
    ensure_database(spark, ref.layer)
    props: dict[str, str] = {}
    if cdf:
        props["delta.enableChangeDataFeed"] = "true"
    props.update(extra_properties or {})
    spark.sql(ddl_for(name, schema, ref.path, partition_by=partition_by, properties=props))
    return ref


def table_exists(spark: SparkSession, name: str) -> bool:
    try:
        return spark._jsparkSession.catalog().tableExists(name)
    except Exception:  # pragma: no cover - defensive
        try:
            spark.table(name)
            return True
        except Exception:
            return False


def table_columns(spark: SparkSession, name: str) -> list[str]:
    return list(spark.table(name).columns)


# --------------------------------------------------------------------------
# Writes
# --------------------------------------------------------------------------


def merge_upsert(
    spark: SparkSession,
    target: str,
    source: DataFrame,
    on: Iterable[str],
    *,
    update_condition: str | None = None,
    insert: bool = True,
) -> dict[str, int]:
    """Run ``MERGE INTO target USING source ON keys`` (idempotent upsert).

    ``update_condition`` is an optional boolean SQL expression (columns of the
    target are aliased ``t``, the source ``s``) guarding the UPDATE branch -- this
    is what makes late-arriving data lose to newer data.

    Returns Delta's own MERGE metrics (``num_inserted_rows`` etc.) so callers can
    report *measured* insert/update counts instead of inferring them.
    """
    view = f"_src_{re.sub(r'[^0-9a-zA-Z_]', '_', target)}"
    source.createOrReplaceTempView(view)
    on_clause = " AND ".join(f"t.`{c}` <=> s.`{c}`" for c in on)
    clauses = []
    if update_condition:
        clauses.append(f"WHEN MATCHED AND {update_condition} THEN UPDATE SET *")
    else:
        clauses.append("WHEN MATCHED THEN UPDATE SET *")
    if insert:
        clauses.append("WHEN NOT MATCHED THEN INSERT *")
    result = spark.sql(f"MERGE INTO {target} t USING {view} s ON {on_clause} " + " ".join(clauses))
    metrics: dict[str, int] = {}
    for row in result.collect():
        data = row.asDict(recursive=True)
        for key, value in data.items():
            if isinstance(value, int):
                metrics[key] = value
    spark.catalog.dropTempView(view)
    return metrics


def overwrite_delta(spark: SparkSession, name: str, df: DataFrame) -> None:
    """Full overwrite of an existing table (used by the recomputed gold marts).

    The write goes straight to the table's ``LOCATION`` instead of through
    ``saveAsTable`` so no metastore ``ALTER TABLE`` is attempted; the pre-created
    table schema (and therefore schema enforcement) still applies.
    """
    ref = TableRef(name)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "false").option(
        "path", ref.path
    ).save()


# --------------------------------------------------------------------------
# Introspection
# --------------------------------------------------------------------------


def row_count(spark: SparkSession, name: str) -> int:
    return spark.table(name).count()


def delta_history(spark: SparkSession, name: str, limit: int = 50) -> list[dict]:
    rows = spark.sql(f"DESCRIBE HISTORY {name} LIMIT {limit}").collect()
    out: list[dict] = []
    for r in rows:
        d = r.asDict(recursive=True)
        out.append(d)
    return out


def delta_version(spark: SparkSession, name: str) -> int:
    rows = spark.sql(f"DESCRIBE HISTORY {name} LIMIT 1").collect()
    return int(rows[0][0]) if rows else -1


def last_commit(spark: SparkSession, name: str) -> tuple[int, datetime | None]:
    rows = spark.sql(f"DESCRIBE HISTORY {name} LIMIT 1").collect()
    if not rows:
        return -1, None
    ts = rows[0]["timestamp"]
    return int(rows[0][0]), ts


def table_size_bytes(spark: SparkSession, name: str) -> int:
    """Total on-disk bytes of the table's active files."""
    rows = spark.sql(f"DESCRIBE DETAIL {name}").collect()
    if not rows:
        return 0
    return int(rows[0]["sizeInBytes"] or 0)


def column_types(spark: SparkSession, name: str) -> dict[str, str]:
    """``{column: spark-sql-type}`` for a live Delta table."""
    return {f.name: f.dataType.simpleString() for f in spark.table(name).schema.fields}


def hash_expr(columns: Iterable[str], prefix: str | None = None) -> F.Column:
    """Deterministic SHA-256 record hash over the given columns."""
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("\\N")) for c in columns]
    joined = F.concat_ws("||", *parts)
    if prefix:
        joined = F.concat_ws("||", F.lit(prefix), joined)
    return F.sha2(joined, 256)
