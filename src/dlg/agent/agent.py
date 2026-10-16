"""The agent loop: plan -> tool call -> observe, with everything audited.

The loop is deliberately small and bounded (``MAX_STEPS = 4``).  Safety does not
depend on the planner being well behaved:

* the question is checked against :mod:`dlg.agent.policy` *before* any planning;
* every tool argument is validated by a pydantic model;
* ``run_sql`` goes through the SQL guard (single SELECT, granted objects only,
  no PII columns, LIMIT injected);
* every step -- tool call or refusal -- is written to ``ops.agent_audit`` so the
  refusal rate, tool error rate and latency can be monitored in production.
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from dlg.agent.llm import TOOL_SPECS, AgentPlan, LLMProvider, StubProvider
from dlg.agent.policy import classify_question
from dlg.agent.tools import TOOL_NAMES, ToolResult, execute_tool
from dlg.config import Settings, get_settings
from dlg.governance.catalog import Catalog, load_catalog
from dlg.lakehouse import ensure_table, new_batch_id, overwrite_delta

__all__ = ["AgentResponse", "AgentStep", "ask", "AUDIT_TABLE", "AUDIT_SCHEMA", "MAX_STEPS"]

MAX_STEPS = 4
AUDIT_TABLE = "ops.agent_audit"

AUDIT_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("ts", TimestampType(), False),
        StructField("step", IntegerType(), False),
        StructField("question", StringType(), False),
        StructField("role", StringType(), False),
        StructField("tool", StringType(), True),
        StructField("args", StringType(), True),
        StructField("status", StringType(), False),
        StructField("reason_code", StringType(), True),
        StructField("latency_ms", IntegerType(), False),
        StructField("rows_returned", LongType(), False),
    ]
)


@dataclass
class AgentStep:
    step: int
    kind: str
    tool: str | None = None
    args: dict = field(default_factory=dict)
    status: str = "ok"
    reason_code: str | None = None
    reason: str | None = None
    rows_returned: int = 0
    latency_ms: int = 0
    sql: str | None = None

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass
class AgentResponse:
    run_id: str
    question: str
    role: str
    answer: str
    status: str
    reason_code: str | None = None
    reason: str | None = None
    tool_used: str | None = None
    sql: str | None = None
    rows: list[dict] = field(default_factory=list)
    row_count: int = 0
    steps: list[AgentStep] = field(default_factory=list)
    latency_ms: int = 0

    @property
    def refused(self) -> bool:
        return self.status == "refused"

    def as_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "question": self.question,
            "role": self.role,
            "answer": self.answer,
            "status": self.status,
            "reason_code": self.reason_code,
            "reason": self.reason,
            "tool_used": self.tool_used,
            "sql": self.sql,
            "rows": self.rows,
            "row_count": self.row_count,
            "steps": [s.as_dict() for s in self.steps],
            "latency_ms": self.latency_ms,
        }


def _write_audit(
    spark: SparkSession,
    run_id: str,
    question: str,
    role: str,
    steps: list[AgentStep],
    settings: Settings,
) -> None:
    ensure_table(spark, AUDIT_TABLE, AUDIT_SCHEMA, cdf=True, settings=settings)
    ts = datetime.now(UTC).replace(tzinfo=None)
    rows = [
        (
            run_id,
            ts,
            step.step,
            question,
            role,
            step.tool,
            json.dumps(step.args, default=str),
            step.status,
            step.reason_code,
            step.latency_ms,
            step.rows_returned,
        )
        for step in steps
    ]
    existing = spark.table(AUDIT_TABLE) if spark.catalog.tableExists(AUDIT_TABLE) else None
    new = spark.createDataFrame(rows, AUDIT_SCHEMA)
    if existing is not None and "_record_hash" not in existing.columns:
        # keep history: append rather than overwrite, so the audit trail is complete
        combined = existing.unionByName(new)
    else:  # pragma: no cover - first write
        combined = new
    overwrite_delta(spark, AUDIT_TABLE, combined)


def ask(
    spark: SparkSession,
    question: str,
    role: str,
    *,
    provider: LLMProvider | None = None,
    settings: Settings | None = None,
    catalog: Catalog | None = None,
    run_id: str | None = None,
    persist_audit: bool = True,
) -> AgentResponse:
    """Answer ``question`` as ``role``, with policy checks and a full audit trail."""
    started = time.perf_counter()
    settings = settings or get_settings()
    catalog = catalog or load_catalog(settings=settings)
    provider = provider or StubProvider()
    run_id = run_id or new_batch_id("agent")

    steps: list[AgentStep] = []
    response = AgentResponse(run_id=run_id, question=question, role=role, answer="", status="ok")

    # ---- 1. question-level policy -----------------------------------------
    decision = classify_question(question)
    if decision.refused:
        steps.append(
            AgentStep(
                step=1,
                kind="refusal",
                status="refused",
                reason_code=decision.reason_code,
                reason=decision.reason,
                latency_ms=int((time.perf_counter() - started) * 1000),
            )
        )
        response.status = "refused"
        response.reason_code = decision.reason_code
        response.reason = decision.reason
        response.answer = f"Refused ({decision.reason_code}): {decision.reason}"
        response.steps = steps
        response.latency_ms = int((time.perf_counter() - started) * 1000)
        if persist_audit:
            _write_audit(spark, run_id, question, role, steps, settings)
        return response

    # ---- 2. plan -> act -> observe ----------------------------------------
    observations: list[dict] = []
    plan: AgentPlan | None = None
    for step_number in range(1, MAX_STEPS + 1):
        step_started = time.perf_counter()
        plan = provider.plan(question, role, tools=TOOL_SPECS, observations=observations or None)

        if plan.kind == "answer":
            step = AgentStep(
                step=step_number,
                kind="answer",
                status="ok",
                latency_ms=int((time.perf_counter() - step_started) * 1000),
            )
            steps.append(step)
            response.answer = plan.summary or "no answer produced"
            break

        if plan.kind == "refusal":
            step = AgentStep(
                step=step_number,
                kind="refusal",
                status="refused",
                reason_code=plan.reason_code or "AMBIGUOUS",
                reason=plan.reason,
                latency_ms=int((time.perf_counter() - step_started) * 1000),
            )
            steps.append(step)
            response.status = "refused"
            response.reason_code = step.reason_code
            response.reason = step.reason
            response.answer = f"Refused ({step.reason_code}): {step.reason}"
            break

        tool = plan.tool or ""
        result: ToolResult = execute_tool(spark, tool, plan.args or {}, role, catalog)
        step = AgentStep(
            step=step_number,
            kind="tool",
            tool=tool,
            args=plan.args or {},
            status=result.status,
            reason_code=result.reason_code,
            reason=result.reason,
            rows_returned=result.row_count,
            latency_ms=result.latency_ms,
            sql=result.sql,
        )
        steps.append(step)

        if result.status == "refused":
            response.status = "refused"
            response.reason_code = result.reason_code
            response.reason = result.reason
            response.answer = f"Refused ({result.reason_code}): {result.reason}"
            break

        response.tool_used = tool
        response.sql = result.sql
        response.rows = result.rows[:10]
        response.row_count = result.row_count
        observations.append(
            {
                "tool": tool,
                "status": result.status,
                "row_count": result.row_count,
                "rows": result.rows[:3],
            }
        )
    else:  # pragma: no cover - loop exhausted
        response.status = "refused"
        response.reason_code = "AMBIGUOUS"
        response.reason = f"could not resolve the question within {MAX_STEPS} steps"
        response.answer = f"Refused (AMBIGUOUS): {response.reason}"

    if response.status == "ok" and not response.answer:
        response.answer = f"{response.row_count} row(s) returned by {response.tool_used}"

    response.steps = steps
    response.latency_ms = int((time.perf_counter() - started) * 1000)
    if persist_audit:
        _write_audit(spark, run_id, question, role, steps, settings)
    return response


def recent_audit(spark: SparkSession, limit: int = 50) -> list[dict]:
    """The most recent audit rows, newest first."""
    if not spark.catalog.tableExists(AUDIT_TABLE):
        return []
    rows = spark.sql(
        f"SELECT * FROM {AUDIT_TABLE} ORDER BY ts DESC, step DESC LIMIT {int(limit)}"
    ).collect()
    out = []
    for row in rows:
        record = row.asDict(recursive=True)
        for key, value in record.items():
            if isinstance(value, datetime):
                record[key] = value.isoformat()
        out.append(record)
    return out


__all__ += ["recent_audit", "TOOL_NAMES"]
