"""FastAPI service exposing the governed analytics agent.

Endpoints:

* ``POST /ask``   -- ``{"question": "...", "role": "..."}``; the role may also be
  supplied via the ``X-DLG-Role`` header. The header, when present, wins, so a
  body cannot silently escalate privileges.
* ``GET /health`` -- liveness plus the loaded tool list and the default role.
* ``GET /tools``  -- the typed tool registry (name, description, input model).
* ``GET /audit``  -- recent rows from ``ops.agent_audit``.

Run it with ``make serve`` (uvicorn on 127.0.0.1:8000).
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

from dlg.agent.agent import ask, recent_audit
from dlg.agent.llm import TOOL_SPECS
from dlg.agent.tools import (
    DescribeTableInput,
    FreshnessInput,
    GetMetricInput,
    ListTablesInput,
    RetentionCohortInput,
    RunSqlInput,
)
from dlg.config import get_settings
from dlg.governance.access import load_roles
from dlg.spark import get_spark

__all__ = ["app", "AskRequest", "AskResponse"]

DEFAULT_ROLE = "agent_service"

app = FastAPI(
    title="DeltaLakehouseGovernance — governed analytics agent",
    version="0.1.0",
    description=(
        "Read-only agentic analytics over the governed gold marts. Every request is "
        "policy-checked, SQL-guarded and written to ops.agent_audit."
    ),
)

_INPUT_MODELS = {
    "list_tables": ListTablesInput,
    "describe_table": DescribeTableInput,
    "run_sql": RunSqlInput,
    "get_metric": GetMetricInput,
    "retention_cohort": RetentionCohortInput,
    "check_data_freshness": FreshnessInput,
}


class AskRequest(BaseModel):
    question: str = Field(min_length=1)
    role: str | None = None


class AskResponse(BaseModel):
    run_id: str
    question: str
    role: str
    status: str
    answer: str
    reason_code: str | None = None
    reason: str | None = None
    tool_used: str | None = None
    sql: str | None = None
    row_count: int = 0
    rows: list[dict[str, Any]] = Field(default_factory=list)
    steps: list[dict[str, Any]] = Field(default_factory=list)
    latency_ms: int = 0


@app.get("/health")
def health() -> dict[str, Any]:
    settings = get_settings()
    roles = load_roles()
    return {
        "status": "ok",
        "default_role": DEFAULT_ROLE,
        "roles": roles.role_names,
        "tools": sorted(TOOL_SPECS),
        "provider": settings.llm_model if settings.llm_base_url else "stub-rule-based",
        "lake_root": settings.lake_root,
    }


@app.get("/tools")
def tools() -> dict[str, Any]:
    registry = []
    for name, description in sorted(TOOL_SPECS.items()):
        model = _INPUT_MODELS[name]
        registry.append(
            {
                "name": name,
                "description": description,
                "input_schema": model.model_json_schema(),
            }
        )
    return {"count": len(registry), "tools": registry}


@app.get("/audit")
def audit(limit: int = 50) -> dict[str, Any]:
    spark = get_spark("dlg-agent-audit")
    rows = recent_audit(spark, limit=limit)
    return {"count": len(rows), "rows": rows}


@app.post("/ask", response_model=AskResponse)
def ask_endpoint(
    request: AskRequest,
    x_dlg_role: str | None = Header(default=None, alias="X-DLG-Role"),
) -> AskResponse:
    role = x_dlg_role or request.role or DEFAULT_ROLE
    # A role supplied in the body is accepted only when it is a known role; the
    # header always takes precedence so a caller cannot escalate via the payload.
    if (
        x_dlg_role is None
        and request.role is not None
        and request.role not in load_roles().role_names
    ):
        raise HTTPException(status_code=400, detail=f"unknown role {request.role!r}")

    spark = get_spark("dlg-agent")
    response = ask(spark, request.question, role)
    return AskResponse(**response.as_dict())
