"""Typed, read-only tools exposed to the agent.

Every tool is a pydantic-validated function with an explicit input and output
model, and every one is **read-only**.  ``run_sql`` additionally passes through a
SQL guard that:

1. strips comments and requires exactly one statement;
2. requires ``SELECT`` (optionally with a leading ``WITH``) and rejects any DDL /
   DML / admin verb, so ``DROP``/``DELETE``/``MERGE``/``INSERT`` are impossible;
3. extracts every referenced object -- skipping CTE names and aliases -- and asks
   :mod:`dlg.governance.access` whether the caller's role may ``SELECT`` it.  The
   ``agent_service`` role is scoped to ``secure.*`` by the access matrix, so a
   reference to ``bronze.sessions`` or ``silver.sessions`` is refused with
   ``UNGRANTED_OBJECT``;
4. refuses any reference to a column classified ``PII`` on the referenced object
   with ``PII_BLOCKED`` (masked governed views are exempt: the mask *is* the
   control);
5. injects a ``LIMIT`` if the query has none, so no tool call can stream the
   whole lake into a prompt.

The guard returns a *structured* refusal rather than raising for policy problems,
so the agent loop can record a reason code in the audit table.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from dlg.agent.policy import PII_BLOCKED, UNGRANTED_OBJECT, UNSAFE_SQL
from dlg.config import Settings, get_settings
from dlg.governance.access import check_access, load_roles
from dlg.governance.catalog import Catalog, load_catalog

__all__ = [
    "SqlGuardResult",
    "guard_sql",
    "ToolResult",
    "TOOL_NAMES",
    "list_tables",
    "describe_table",
    "run_sql",
    "get_metric",
    "retention_cohort",
    "check_data_freshness",
    "execute_tool",
]

DEFAULT_LIMIT = 100
MAX_LIMIT = 1000

TOOL_NAMES = (
    "list_tables",
    "describe_table",
    "run_sql",
    "get_metric",
    "retention_cohort",
    "check_data_freshness",
)

#: verbs that may never appear in agent SQL
FORBIDDEN_KEYWORDS = (
    "insert",
    "update",
    "delete",
    "merge",
    "drop",
    "alter",
    "create",
    "truncate",
    "replace",
    "grant",
    "revoke",
    "copy",
    "export",
    "load",
    "call",
    "set",
    "use",
    "cache",
    "uncache",
    "refresh",
    "analyze",
    "optimize",
    "vacuum",
    "restore",
    "overwrite",
    "msck",
    "repair",
    "add",
    "dfs",
    "hadoop",
    "system",
    "python",
    "scala",
    "jar",
    "file",
    "archive",
    "unarchive",
    "kill",
    "reset",
    "show",
    "describe",
    "explain",
    "clear",
)

#: metrics the semantic tool understands
METRICS = (
    "dau",
    "revenue",
    "arpdau",
    "conversion_rate",
    "paying_users",
    "purchases",
    "sessions",
    "retention_d1",
    "retention_d7",
    "retention_d30",
    "churn_rate",
    "lifetime_spend",
)

#: tables whose PII columns are already masked by a governed view
MASKED_VIEWS = ("secure.",)


# --------------------------------------------------------------------------
# pydantic models
# --------------------------------------------------------------------------


class ToolResult(BaseModel):
    tool: str
    status: Literal["ok", "refused", "error"] = "ok"
    reason_code: str | None = None
    reason: str | None = None
    rows: list[dict[str, Any]] = Field(default_factory=list)
    row_count: int = 0
    sql: str | None = None
    latency_ms: int = 0


class ListTablesInput(BaseModel):
    layer: str | None = Field(default=None, description="Optional layer filter")


class DescribeTableInput(BaseModel):
    table: str


class RunSqlInput(BaseModel):
    sql: str
    limit: int = DEFAULT_LIMIT


class GetMetricInput(BaseModel):
    metric: str
    date_range: list[str] | None = Field(
        default=None, description="[start_date, end_date] as YYYY-MM-DD"
    )


class RetentionCohortInput(BaseModel):
    cohort_date: str | None = None
    horizon: Literal["d1", "d7", "d30", "all"] = "all"


class FreshnessInput(BaseModel):
    table: str


# --------------------------------------------------------------------------
# SQL guard
# --------------------------------------------------------------------------


@dataclass
class SqlGuardResult:
    allowed: bool
    reason_code: str | None = None
    reason: str = ""
    sql: str = ""
    objects: list[str] = field(default_factory=list)
    limit_injected: bool = False

    def as_dict(self) -> dict:
        return {
            "allowed": self.allowed,
            "reason_code": self.reason_code,
            "reason": self.reason,
            "objects": self.objects,
            "limit_injected": self.limit_injected,
        }


_COMMENT_BLOCK = re.compile(r"/\*.*?\*/", re.DOTALL)
_COMMENT_LINE = re.compile(r"--[^\n]*")
_CTE = re.compile(r"(?:with|,)\s+([a-z_][a-z0-9_]*)\s+as\s*\(", re.IGNORECASE)
_FROM_JOIN = re.compile(
    r"\b(?:from|join)\s+([`\w.\[\]\"-]+)",
    re.IGNORECASE,
)
_HAS_LIMIT = re.compile(r"\blimit\s+\d+\s*$", re.IGNORECASE)
_FIRST_KEYWORD = re.compile(r"^\s*(select|with)\b", re.IGNORECASE)


def _strip_sql(sql: str) -> str:
    sql = _COMMENT_BLOCK.sub(" ", sql or "")
    sql = _COMMENT_LINE.sub(" ", sql)
    return re.sub(r"\s+", " ", sql).strip()


def _statements(sql: str) -> list[str]:
    parts = [p.strip() for p in sql.split(";")]
    return [p for p in parts if p]


def _normalise_object(raw: str) -> str:
    return raw.strip().strip("`").strip('"').strip("[]")


def _referenced_objects(sql: str) -> list[str]:
    cte_names = {m.group(1).lower() for m in _CTE.finditer(sql)}
    objects: list[str] = []
    for match in _FROM_JOIN.finditer(sql):
        name = _normalise_object(match.group(1))
        if not name or name.lower() in cte_names:
            continue
        if name.startswith("("):
            continue
        if "." not in name:
            continue  # a bare name is a CTE or a syntax error
        # strip a trailing alias that was glued on (e.g. `t.col`)
        if name.count(".") > 1:
            name = ".".join(name.split(".")[:2])
        if name not in objects:
            objects.append(name)
    return objects


def guard_sql(
    sql: str,
    role: str,
    *,
    limit: int = DEFAULT_LIMIT,
    catalog: Catalog | None = None,
    settings: Settings | None = None,
) -> SqlGuardResult:
    """Validate and policy-check a read-only SQL statement."""
    settings = settings or get_settings()
    catalog = catalog or load_catalog(settings=settings)
    matrix = load_roles(settings=settings)

    clean = _strip_sql(sql)
    if not clean:
        return SqlGuardResult(False, UNSAFE_SQL, "no SQL was provided")

    statements = _statements(clean)
    if len(statements) != 1:
        return SqlGuardResult(
            False,
            UNSAFE_SQL,
            f"exactly one statement is allowed, found {len(statements)}",
        )
    statement = statements[0]

    if not _FIRST_KEYWORD.match(statement):
        return SqlGuardResult(
            False,
            UNSAFE_SQL,
            "only SELECT statements (optionally starting with WITH) are allowed",
        )

    lowered = statement.lower()
    for keyword in FORBIDDEN_KEYWORDS:
        if re.search(rf"(?<![a-z0-9_]){keyword}(?![a-z0-9_])", lowered):
            return SqlGuardResult(
                False,
                UNSAFE_SQL,
                f"'{keyword}' is not permitted in agent SQL (read-only SELECT only)",
            )

    objects = _referenced_objects(statement)
    if not objects:
        return SqlGuardResult(
            False,
            UNSAFE_SQL,
            "the query does not reference any fully qualified table or view",
        )

    # ---- access control -------------------------------------------------
    for obj in objects:
        decision = matrix.check(role, obj, "SELECT")
        if not decision.allowed:
            return SqlGuardResult(
                False,
                UNGRANTED_OBJECT,
                f"{role} may not SELECT {obj}: {decision.reason}",
                objects=objects,
            )

    # ---- PII ------------------------------------------------------------
    for obj in objects:
        if obj.startswith(MASKED_VIEWS):
            continue  # PII is already masked by the view definition
        table = catalog.tables.get(obj)
        if table is None:
            continue
        for pii_column in table.pii_columns:
            if re.search(rf"(?<![a-z0-9_]){re.escape(pii_column)}(?![a-z0-9_])", lowered):
                return SqlGuardResult(
                    False,
                    PII_BLOCKED,
                    (
                        f"{obj}.{pii_column} is classified PII and is not readable "
                        f"through this service; use the governed secure.* view instead"
                    ),
                    objects=objects,
                )

    # ---- LIMIT injection ------------------------------------------------
    limit = max(1, min(int(limit or DEFAULT_LIMIT), MAX_LIMIT))
    injected = False
    if not _HAS_LIMIT.search(statement):
        statement = f"{statement} LIMIT {limit}"
        injected = True

    return SqlGuardResult(True, None, "ok", statement, objects, injected)


# --------------------------------------------------------------------------
# tool implementations
# --------------------------------------------------------------------------


def _frame_to_rows(df, cap: int = MAX_LIMIT) -> list[dict[str, Any]]:
    rows = []
    for row in df.limit(cap).collect():
        record = {}
        for key, value in row.asDict(recursive=True).items():
            record[key] = value.isoformat() if isinstance(value, (date, datetime)) else value
        rows.append(record)
    return rows


def list_tables(
    spark: SparkSession, params: ListTablesInput, role: str, catalog: Catalog | None = None
) -> ToolResult:
    catalog = catalog or load_catalog()
    matrix = load_roles()
    rows = []
    for table in catalog.tables.values():
        decision = matrix.check(role, table.name, "SELECT")
        if not decision.allowed:
            continue
        if params.layer and table.layer != params.layer:
            continue
        rows.append(
            {
                "table": table.name,
                "layer": table.layer,
                "domain": table.domain,
                "classification": table.classification,
                "rows": None,
                "description": table.description[:160],
            }
        )
    # governed views are just as visible to the agent as marts
    from dlg.governance.policies import GOVERNED_VIEW_LINEAGE

    for view in GOVERNED_VIEW_LINEAGE:
        decision = matrix.check(role, view, "SELECT")
        if not decision.allowed:
            continue
        if params.layer and params.layer != "secure":
            continue
        rows.append(
            {
                "table": view,
                "layer": "secure",
                "domain": "governed",
                "classification": "INTERNAL",
                "rows": None,
                "description": "governed view with PII masking / row filters applied",
            }
        )
    return ToolResult(tool="list_tables", rows=rows, row_count=len(rows))


def describe_table(
    spark: SparkSession, params: DescribeTableInput, role: str, catalog: Catalog | None = None
) -> ToolResult:
    catalog = catalog or load_catalog()
    name = params.table.strip().strip("`")
    decision = check_access(role, name, "SELECT")
    if not decision.allowed:
        return ToolResult(
            tool="describe_table",
            status="refused",
            reason_code=UNGRANTED_OBJECT,
            reason=f"{role} may not read {name}: {decision.reason}",
        )
    try:
        physical = spark.table(name).schema
    except Exception as exc:
        return ToolResult(
            tool="describe_table",
            status="error",
            reason_code="UNKNOWN_TABLE",
            reason=f"{name} could not be resolved: {type(exc).__name__}",
        )
    declared = catalog.tables.get(name)
    rows = []
    for struct_field in physical.fields:
        spec = declared.column(struct_field.name) if declared else None
        rows.append(
            {
                "column": struct_field.name,
                "type": struct_field.dataType.simpleString(),
                "nullable": struct_field.nullable,
                "classification": spec.classification if spec else "UNKNOWN",
                "description": spec.description if spec else "",
            }
        )
    return ToolResult(tool="describe_table", rows=rows, row_count=len(rows))


def run_sql(
    spark: SparkSession,
    params: RunSqlInput,
    role: str,
    catalog: Catalog | None = None,
) -> ToolResult:
    guard = guard_sql(params.sql, role, limit=params.limit, catalog=catalog)
    if not guard.allowed:
        return ToolResult(
            tool="run_sql",
            status="refused",
            reason_code=guard.reason_code,
            reason=guard.reason,
        )
    try:
        df = spark.sql(guard.sql)
    except Exception as exc:  # malformed SQL that passed the guard
        return ToolResult(
            tool="run_sql",
            status="refused",
            reason_code=UNSAFE_SQL,
            reason=f"query could not be planned: {type(exc).__name__}: {str(exc)[:200]}",
            sql=guard.sql,
        )
    rows = _frame_to_rows(df)
    return ToolResult(tool="run_sql", rows=rows, row_count=len(rows), sql=guard.sql)


_METRIC_SQL = {
    "dau": "SELECT event_date, dau FROM secure.v_monetization_daily",
    "revenue": "SELECT event_date, revenue_usd FROM secure.v_monetization_daily",
    "arpdau": "SELECT event_date, arpdau FROM secure.v_monetization_daily",
    "conversion_rate": "SELECT event_date, conversion_rate FROM secure.v_monetization_daily",
    "paying_users": "SELECT event_date, paying_users FROM secure.v_monetization_daily",
    "purchases": "SELECT event_date, purchases FROM secure.v_monetization_daily",
    "sessions": "SELECT event_date, sessions FROM secure.v_monetization_daily",
    "retention_d1": "SELECT cohort_date, cohort_size, d1_retention FROM secure.v_retention_cohort",
    "retention_d7": "SELECT cohort_date, cohort_size, d7_retention FROM secure.v_retention_cohort",
    "retention_d30": "SELECT cohort_date, cohort_size, d30_retention FROM secure.v_retention_cohort",
    "churn_rate": (
        "SELECT region, avg(CASE WHEN churn_flag THEN 1.0 ELSE 0.0 END) AS churn_rate, "
        "count(*) AS players FROM secure.v_dim_player_analyst GROUP BY region"
    ),
    "lifetime_spend": (
        "SELECT region, avg(lifetime_spend) AS avg_lifetime_spend, count(*) AS players "
        "FROM secure.v_dim_player_analyst GROUP BY region"
    ),
}


def get_metric(
    spark: SparkSession,
    params: GetMetricInput,
    role: str,
    catalog: Catalog | None = None,
) -> ToolResult:
    metric = params.metric.strip().lower()
    if metric not in _METRIC_SQL:
        return ToolResult(
            tool="get_metric",
            status="refused",
            reason_code="AMBIGUOUS",
            reason=(
                f"unknown metric {params.metric!r}. Known metrics: {', '.join(sorted(_METRIC_SQL))}"
            ),
        )
    sql = _METRIC_SQL[metric]
    if params.date_range:
        if len(params.date_range) != 2:
            return ToolResult(
                tool="get_metric",
                status="refused",
                reason_code="AMBIGUOUS",
                reason="date_range must be [start_date, end_date]",
            )
        start, end = params.date_range
        date_column = "cohort_date" if metric.startswith("retention_") else "event_date"
        sql = (
            f"{sql} WHERE {date_column} BETWEEN DATE'{start}' AND DATE'{end}'"
            if " WHERE " not in sql
            else f"{sql} AND {date_column} BETWEEN DATE'{start}' AND DATE'{end}'"
        )
    result = run_sql(spark, RunSqlInput(sql=sql), role, catalog)
    result.tool = "get_metric"
    return result


def retention_cohort(
    spark: SparkSession,
    params: RetentionCohortInput,
    role: str,
    catalog: Catalog | None = None,
) -> ToolResult:
    columns = ["cohort_date", "cohort_size"]
    if params.horizon == "all":
        columns += ["d1_retention", "d7_retention", "d30_retention", "is_matured_d30"]
    else:
        columns += [f"{params.horizon}_retention"]
    sql = f"SELECT {', '.join(columns)} FROM secure.v_retention_cohort"
    if params.cohort_date:
        sql += f" WHERE cohort_date = DATE'{params.cohort_date}'"
    else:
        sql += " ORDER BY cohort_date"
    result = run_sql(spark, RunSqlInput(sql=sql), role, catalog)
    result.tool = "retention_cohort"
    return result


def check_data_freshness(
    spark: SparkSession,
    params: FreshnessInput,
    role: str,
    catalog: Catalog | None = None,
) -> ToolResult:
    catalog = catalog or load_catalog()
    name = params.table.strip().strip("`")
    decision = check_access(role, name, "SELECT")
    if not decision.allowed:
        return ToolResult(
            tool="check_data_freshness",
            status="refused",
            reason_code=UNGRANTED_OBJECT,
            reason=f"{role} may not read {name}: {decision.reason}",
        )
    try:
        df = spark.table(name)
    except Exception:
        return ToolResult(
            tool="check_data_freshness",
            status="error",
            reason_code="UNKNOWN_TABLE",
            reason=f"{name} could not be resolved",
        )
    temporal = next(
        (
            c
            for c in ("_ingest_ts", "_quarantine_ts", "event_date", "cohort_date")
            if c in df.columns
        ),
        None,
    )
    if temporal is None:
        return ToolResult(
            tool="check_data_freshness",
            status="ok",
            rows=[{"table": name, "column": None, "newest": None, "detail": "no temporal column"}],
            row_count=1,
        )
    newest = df.agg(F.max(F.col(temporal)).alias("m")).collect()[0]["m"]
    rows = df.count()
    return ToolResult(
        tool="check_data_freshness",
        rows=[
            {
                "table": name,
                "column": temporal,
                "newest": newest.isoformat() if newest is not None else None,
                "rows": rows,
                "detail": f"newest {temporal} value; {rows} rows in the table",
            }
        ],
        row_count=1,
    )


# --------------------------------------------------------------------------
# dispatch
# --------------------------------------------------------------------------

_DISPATCH = {
    "list_tables": (ListTablesInput, list_tables),
    "describe_table": (DescribeTableInput, describe_table),
    "run_sql": (RunSqlInput, run_sql),
    "get_metric": (GetMetricInput, get_metric),
    "retention_cohort": (RetentionCohortInput, retention_cohort),
    "check_data_freshness": (FreshnessInput, check_data_freshness),
}


def execute_tool(
    spark: SparkSession,
    tool: str,
    args: dict[str, Any],
    role: str,
    catalog: Catalog | None = None,
) -> ToolResult:
    """Validate ``args`` against the tool's model and run it."""
    started = time.perf_counter()
    if tool not in _DISPATCH:
        return ToolResult(
            tool=tool,
            status="refused",
            reason_code="AMBIGUOUS",
            reason=f"unknown tool {tool!r}; available: {', '.join(TOOL_NAMES)}",
        )
    model, fn = _DISPATCH[tool]
    try:
        params = model(**args)
    except Exception as exc:
        return ToolResult(
            tool=tool,
            status="refused",
            reason_code="AMBIGUOUS",
            reason=f"invalid arguments for {tool}: {str(exc)[:200]}",
        )
    result = fn(spark, params, role, catalog)
    result.latency_ms = int((time.perf_counter() - started) * 1000)
    return result
