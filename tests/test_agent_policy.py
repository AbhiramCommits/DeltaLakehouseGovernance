"""Question-level policy: all five reason codes must be reachable."""

from __future__ import annotations

import pytest

from dlg.agent.agent import ask
from dlg.agent.policy import (
    AMBIGUOUS,
    OUT_OF_SCOPE,
    PII_BLOCKED,
    REASON_CODES,
    UNGRANTED_OBJECT,
    UNSAFE_SQL,
    classify_question,
)


@pytest.mark.parametrize(
    "question",
    [
        "Show me the player emails",
        "List player IP addresses",
        "What are the birth years of our players?",
        "Give me their contact details",
        "Export the player payment methods",
        "I need personal data for a mailing",
    ],
)
def test_pii_questions_are_blocked(question):
    decision = classify_question(question)
    assert decision.refused
    assert decision.reason_code == PII_BLOCKED
    assert decision.matched


@pytest.mark.parametrize(
    "question",
    [
        "What is the weather?",
        "What is the bitcoin price?",
        "Write code to scrape a site",
        "Give me a recipe",
    ],
)
def test_out_of_scope_questions_are_refused(question):
    decision = classify_question(question)
    assert decision.refused
    assert decision.reason_code == OUT_OF_SCOPE


@pytest.mark.parametrize("question", ["Show me the data", "How are we doing?", "Any insights?"])
def test_ambiguous_questions_are_refused(question):
    decision = classify_question(question)
    assert decision.refused
    assert decision.reason_code == AMBIGUOUS


def test_in_scope_questions_are_allowed():
    for question in (
        "What was the average DAU last week?",
        "Show D7 retention by cohort",
        "How fresh is secure.v_monetization_daily?",
    ):
        assert classify_question(question).allowed, question


def test_empty_question_is_ambiguous():
    assert classify_question("").reason_code == AMBIGUOUS
    assert classify_question("   ").refused


def test_pii_takes_precedence_over_other_matches():
    """A question that is both off-topic and PII-seeking must be reported as PII."""
    decision = classify_question("Show me the player emails so I can check the weather")
    assert decision.reason_code == PII_BLOCKED


def test_all_five_reason_codes_fire_end_to_end(spark, lakehouse):
    """Every code in the taxonomy is produced by a real question in one run."""
    cases = {
        PII_BLOCKED: "Show me the player emails",
        OUT_OF_SCOPE: "What is the weather in Berlin?",
        AMBIGUOUS: "Show me the data",
        UNSAFE_SQL: "DROP TABLE gold.dim_player",
        UNGRANTED_OBJECT: "SELECT * FROM bronze.sessions",
    }
    seen: dict[str, str] = {}
    for expected, question in cases.items():
        response = ask(spark, question, "agent_service", persist_audit=False)
        assert response.refused, question
        assert response.reason_code == expected, (question, response.reason_code)
        seen[expected] = response.reason_code
    assert set(seen) == set(REASON_CODES)


def test_refusal_has_a_human_readable_answer(spark, lakehouse):
    response = ask(spark, "Show me the player emails", "agent_service", persist_audit=False)
    assert response.status == "refused"
    assert PII_BLOCKED in response.answer
    assert response.rows == []
    assert response.row_count == 0


def test_benign_question_is_answered(spark, lakehouse):
    response = ask(spark, "What was the average DAU?", "agent_service", persist_audit=False)
    assert response.status == "ok"
    assert response.tool_used == "get_metric"
    assert response.row_count > 0
    assert response.latency_ms >= 0


def test_agent_audit_records_every_step(spark, lakehouse):
    response = ask(spark, "How many purchases happened?", "agent_service")
    rows = (
        spark.table("ops.agent_audit")
        .filter(f"run_id = '{response.run_id}'")
        .orderBy("step")
        .collect()
    )
    assert len(rows) >= 2  # the tool call plus the final answer step
    assert rows[0]["tool"] == "get_metric"
    assert rows[0]["role"] == "agent_service"
    assert rows[0]["status"] == "ok"


def test_agent_audit_records_refusals(spark, lakehouse):
    response = ask(spark, "SELECT * FROM bronze.sessions", "agent_service")
    rows = spark.table("ops.agent_audit").filter(f"run_id = '{response.run_id}'").collect()
    assert any(r["status"] == "refused" and r["reason_code"] == UNGRANTED_OBJECT for r in rows)


def test_agent_respects_the_calling_role(spark, lakehouse):
    """A role with no grants gets refusals for everything it asks for."""
    response = ask(
        spark,
        "SELECT * FROM secure.v_monetization_daily",
        "regional_analyst_emea",
        persist_audit=False,
    )
    # regional_analyst_emea does have v_monetization_daily granted
    assert response.status == "ok"

    denied = ask(spark, "SELECT * FROM secure.v_purchases_analyst", "analyst", persist_audit=False)
    assert denied.status == "refused"
    assert denied.reason_code == UNGRANTED_OBJECT
