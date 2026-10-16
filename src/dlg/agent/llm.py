"""LLM providers for the agent planner.

The agent never talks to a model directly; it talks to an :class:`LLMProvider`.
Two implementations ship:

* :class:`StubProvider` -- a deterministic, rule-based planner.  It needs no
  network, no API key and no GPU, and it produces the *same* plan for the same
  question every time, which is what makes the eval suite meaningful in CI.
* :class:`OpenAICompatProvider` -- talks to any OpenAI-compatible
  ``/chat/completions`` endpoint (vLLM, Ollama, Azure OpenAI, OpenAI, ...) using
  ``DLG_LLM_BASE_URL`` / ``DLG_LLM_API_KEY``.  It is never used by the tests.

Both must return an :class:`AgentPlan`, so swapping the provider cannot change
the agent's safety properties: refusals are decided by :mod:`dlg.agent.policy`
and the SQL guard, not by the model.
"""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from typing import Any, Literal, Protocol

from dlg.config import Settings, get_settings

__all__ = [
    "AgentPlan",
    "LLMProvider",
    "StubProvider",
    "OpenAICompatProvider",
    "get_provider",
    "TOOL_SPECS",
]

#: What the planner is allowed to choose between (kept in one place so the
#: system prompt and the stub can never drift apart).
TOOL_SPECS: dict[str, str] = {
    "list_tables": "List the tables and governed views the caller may read.",
    "describe_table": "Describe a table: column names, types and PII classification.",
    "run_sql": "Run one read-only SELECT against the governed secure.* views.",
    "get_metric": "Return a daily metric series (dau, revenue, arpdau, conversion_rate, ...).",
    "retention_cohort": "Return install-cohort retention (D1/D7/D30).",
    "check_data_freshness": "Report how fresh a table is.",
}


@dataclass
class AgentPlan:
    """One planner decision: call a tool, or answer, or refuse."""

    kind: Literal["tool", "answer", "refusal"] = "tool"
    tool: str | None = None
    args: dict[str, Any] = field(default_factory=dict)
    reason_code: str | None = None
    reason: str | None = None
    summary: str | None = None

    def as_dict(self) -> dict:
        return asdict(self)


class LLMProvider(Protocol):
    name: str

    def plan(
        self,
        question: str,
        role: str,
        *,
        tools: dict[str, str],
        observations: list[dict[str, Any]] | None = None,
    ) -> AgentPlan:  # pragma: no cover - protocol
        ...


# --------------------------------------------------------------------------
# lexicons used by the deterministic planner
# --------------------------------------------------------------------------

_METRIC_HINTS: tuple[tuple[tuple[str, ...], str], ...] = (
    (("arpdau", "revenue per daily active", "arpu"), "arpdau"),
    (("conversion", "converting", "payer rate", "paying rate"), "conversion_rate"),
    (("paying user", "payers", "paying_user"), "paying_users"),
    (("revenue", "sales", "gross", "income", "how much money"), "revenue"),
    (("purchase", "transaction", "orders", "sku"), "purchases"),
    (("churn",), "churn_rate"),
    (("lifetime spend", "ltv", "lifetime value"), "lifetime_spend"),
    (("session", "sessions"), "sessions"),
    (("daily active", "dau", "active user", "active play"), "dau"),
)

_RETENTION_HINTS = (
    "retention",
    "cohort",
    "week one",
    "day one",
    "d1",
    "d7",
    "d30",
    "stickiness",
)
_FRESHNESS_HINTS = (
    "fresh",
    "freshness",
    "stale",
    "up to date",
    "up-to-date",
    "lagging",
    "last loaded",
)
_LIST_HINTS = ("what tables", "which tables", "list tables", "available tables", "list the tables")
_DESCRIBE_HINTS = (
    "describe",
    "columns",
    "column",
    "schema",
    "structure of",
    "what fields",
    "which fields",
)
_READ_HINTS = (
    "rows",
    "row from",
    "select",
    "query",
    "read",
    "fetch",
    "return the data",
    "directly",
)
_DATE = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")
_TABLE = re.compile(r"\b((?:bronze|silver|gold|ops|secure)\.[a-z_][a-z0-9_]*)\b", re.IGNORECASE)


class StubProvider:
    """Rule-based planner: deterministic, offline and CI-friendly."""

    name = "stub-rule-based"

    def plan(
        self,
        question: str,
        role: str,
        *,
        tools: dict[str, str],
        observations: list[dict[str, Any]] | None = None,
    ) -> AgentPlan:
        if observations:
            last = observations[-1]
            return AgentPlan(
                kind="answer",
                summary=self._summarise(last),
            )

        text = (question or "").strip()
        lowered = text.lower()
        table = self._table(lowered)

        # Order matters: a destructive verb must be routed to the SQL guard
        # before any "helpful" metric matching can reinterpret it, and an
        # explicit request for columns/schema outranks a topic word like
        # "cohort" appearing in the object name.
        if any(hint in lowered for hint in _LIST_HINTS):
            return AgentPlan(kind="tool", tool="list_tables", args={})

        if any(
            re.search(rf"(?<![a-z0-9_]){verb}(?![a-z0-9_])", lowered)
            for verb in (
                "drop",
                "delete",
                "update",
                "insert",
                "merge",
                "alter",
                "truncate",
                "grant",
                "revoke",
                "create",
            )
        ):
            statement = re.sub(
                r"^(?:please\s+)?(?:run|execute|do)\s+(?:this\s*:?\s*)?",
                "",
                text,
                flags=re.IGNORECASE,
            ).strip()
            return AgentPlan(kind="tool", tool="run_sql", args={"sql": statement})

        if any(hint in lowered for hint in _DESCRIBE_HINTS):
            if not table:
                return AgentPlan(
                    kind="refusal",
                    reason_code="AMBIGUOUS",
                    reason="which table should I describe?",
                )
            return AgentPlan(kind="tool", tool="describe_table", args={"table": table})

        if table and any(hint in lowered for hint in _READ_HINTS):
            # Hand it to the SQL guard rather than answering from the semantic
            # tools, so access control produces the precise reason code.
            return AgentPlan(kind="tool", tool="run_sql", args={"sql": f"SELECT * FROM {table}"})

        if any(hint in lowered for hint in _FRESHNESS_HINTS):
            return AgentPlan(
                kind="tool",
                tool="check_data_freshness",
                args={"table": table or "secure.v_monetization_daily"},
            )

        if "select " in lowered or "sql" in lowered:
            match = re.search(r"(select\s+.+)", text, re.IGNORECASE | re.DOTALL)
            if match:
                return AgentPlan(kind="tool", tool="run_sql", args={"sql": match.group(1).strip()})
            return AgentPlan(
                kind="refusal",
                reason_code="AMBIGUOUS",
                reason="the question mentions SQL but contains no SELECT statement",
            )

        if any(hint in lowered for hint in _RETENTION_HINTS):
            dates = _DATE.findall(text)
            args: dict[str, Any] = {}
            for token in ("d1", "d7", "d30"):
                if token in lowered.replace(" ", ""):
                    args["horizon"] = token
            if dates:
                args["cohort_date"] = dates[0]
            return AgentPlan(kind="tool", tool="retention_cohort", args=args)

        for hints, metric in _METRIC_HINTS:
            if any(hint in lowered for hint in hints):
                args = {"metric": metric}
                dates = _DATE.findall(text)
                if len(dates) >= 2:
                    args["date_range"] = [dates[0], dates[1]]
                elif len(dates) == 1:
                    args["date_range"] = [dates[0], dates[0]]
                return AgentPlan(kind="tool", tool="get_metric", args=args)

        return AgentPlan(
            kind="refusal",
            reason_code="AMBIGUOUS",
            reason=(
                "I could not map that to a metric. Try naming one, for example "
                "'average DAU last week' or 'D7 retention by cohort'."
            ),
        )

    # ------------------------------------------------------------------
    @staticmethod
    def _table(lowered: str) -> str | None:
        match = _TABLE.search(lowered)
        return match.group(1) if match else None

    @staticmethod
    def _summarise(observation: dict[str, Any]) -> str:
        if observation.get("status") == "refused":
            return f"refused: {observation.get('reason_code')} — {observation.get('reason')}"
        rows = observation.get("rows") or []
        if not rows:
            return "the query returned no rows"
        keys = list(rows[0])
        preview = json.dumps(rows[:3], default=str)
        return (
            f"returned {observation.get('row_count', len(rows))} row(s) "
            f"with columns {', '.join(keys)}; first rows: {preview}"
        )


# --------------------------------------------------------------------------
# OpenAI-compatible provider
# --------------------------------------------------------------------------

SYSTEM_PROMPT = """You are a governed analytics planner for a Delta Lakehouse.

You may only call the tools listed below, and only against the governed
`secure.*` views. You must never attempt to bypass masking, row filters or
access control, and you must never write DDL/DML.

Respond with a single JSON object:
  {"tool": "<name>", "args": {...}}            to call a tool
  {"answer": "<text>"}                          to answer directly
  {"refusal": "<reason>"}                       to refuse

Available tools:
{tools}
"""


class OpenAICompatProvider:
    """Talks to any OpenAI-compatible chat-completions endpoint."""

    name = "openai-compatible"

    def __init__(
        self,
        base_url: str | None = None,
        api_key: str | None = None,
        model: str | None = None,
        timeout: float = 30.0,
        settings: Settings | None = None,
    ) -> None:
        settings = settings or get_settings()
        self.base_url = (base_url or settings.llm_base_url or "").rstrip("/")
        self.api_key = api_key or settings.llm_api_key or ""
        self.model = model or settings.llm_model
        self.timeout = timeout
        if not self.base_url:
            raise RuntimeError(
                "DLG_LLM_BASE_URL is not set; either configure it or use the stub provider"
            )

    def plan(
        self,
        question: str,
        role: str,
        *,
        tools: dict[str, str],
        observations: list[dict[str, Any]] | None = None,
    ) -> AgentPlan:
        import httpx

        tool_text = "\n".join(f"- {name}: {desc}" for name, desc in tools.items())
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT.format(tools=tool_text)},
            {"role": "user", "content": f"role={role}\nquestion={question}"},
        ]
        for observation in observations or []:
            messages.append(
                {"role": "user", "content": f"observation={json.dumps(observation, default=str)}"}
            )

        response = httpx.post(
            f"{self.base_url}/chat/completions",
            headers={"Authorization": f"Bearer {self.api_key}"},
            json={"model": self.model, "messages": messages, "temperature": 0},
            timeout=self.timeout,
        )
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"]
        payload = json.loads(content)
        if "tool" in payload:
            return AgentPlan(kind="tool", tool=payload["tool"], args=payload.get("args", {}))
        if "refusal" in payload:
            return AgentPlan(kind="refusal", reason_code="POLICY", reason=payload["refusal"])
        return AgentPlan(kind="answer", summary=payload.get("answer", ""))


def get_provider(name: str | None = None, settings: Settings | None = None) -> LLMProvider:
    """Return the configured provider (defaults to the deterministic stub)."""
    settings = settings or get_settings()
    choice = (name or ("openai" if settings.llm_base_url else "stub")).lower()
    if choice in ("stub", "stub-rule-based", "local"):
        return StubProvider()
    return OpenAICompatProvider(settings=settings)
