"""The SQL guard: DDL, DML, multi-statement, PII, ungranted objects, LIMIT."""

from __future__ import annotations

import pytest

from dlg.agent.policy import PII_BLOCKED, UNGRANTED_OBJECT, UNSAFE_SQL
from dlg.agent.tools import (
    DEFAULT_LIMIT,
    TOOL_NAMES,
    DescribeTableInput,
    FreshnessInput,
    GetMetricInput,
    ListTablesInput,
    RetentionCohortInput,
    RunSqlInput,
    execute_tool,
    guard_sql,
)

ROLE = "agent_service"


# ---------------------------------------------------------------------------
# guard: unsafe SQL
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "DROP TABLE gold.dim_player",
        "DELETE FROM secure.v_fct_player_daily",
        "UPDATE secure.v_monetization_daily SET dau = 0",
        "INSERT INTO secure.v_monetization_daily VALUES (1)",
        "MERGE INTO gold.dim_player USING x ON true",
        "ALTER TABLE gold.dim_player ADD COLUMN x INT",
        "CREATE TABLE gold.new_mart (id INT) USING DELTA",
        "TRUNCATE TABLE gold.fct_player_daily",
        "GRANT SELECT ON gold.dim_player TO analyst",
        "EXPLAIN SELECT * FROM secure.v_monetization_daily",
    ],
)
def test_guard_rejects_ddl_and_dml(sql):
    result = guard_sql(sql, ROLE)
    assert not result.allowed
    assert result.reason_code == UNSAFE_SQL


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM secure.v_monetization_daily; DROP TABLE gold.dim_player",
        "SELECT 1; SELECT 2",
        "SELECT * FROM secure.v_monetization_daily; DELETE FROM silver.sessions",
    ],
)
def test_guard_rejects_multiple_statements(sql):
    result = guard_sql(sql, ROLE)
    assert not result.allowed
    assert result.reason_code == UNSAFE_SQL


def test_guard_rejects_empty_sql():
    assert not guard_sql("   ", ROLE).allowed
    assert not guard_sql("", ROLE).allowed


def test_guard_rejects_sql_without_a_table():
    result = guard_sql("SELECT 1", ROLE)
    assert not result.allowed
    assert result.reason_code == UNSAFE_SQL


# ---------------------------------------------------------------------------
# guard: access control
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "obj",
    [
        "bronze.sessions",
        "silver.sessions",
        "silver.purchases",
        "gold.dim_player",
        "ops.agent_audit",
    ],
)
def test_guard_rejects_ungranted_objects(obj):
    result = guard_sql(f"SELECT * FROM {obj}", ROLE)
    assert not result.allowed
    assert result.reason_code == UNGRANTED_OBJECT


def test_guard_rejects_ungranted_joins():
    result = guard_sql(
        "SELECT * FROM secure.v_monetization_daily m "
        "JOIN silver.sessions s ON s.player_id = m.event_date",
        ROLE,
    )
    assert not result.allowed
    assert result.reason_code == UNGRANTED_OBJECT


def test_guard_allows_granted_views():
    for obj in (
        "secure.v_monetization_daily",
        "secure.v_retention_cohort",
        "secure.v_fct_player_daily",
    ):
        result = guard_sql(f"SELECT * FROM {obj}", ROLE)
        assert result.allowed, (obj, result.reason)


# ---------------------------------------------------------------------------
# guard: PII
# ---------------------------------------------------------------------------


def test_guard_blocks_pii_columns_on_base_tables():
    """For a role that *may* read the base table, PII columns are still refused."""
    result = guard_sql("SELECT player_email FROM silver.sessions", "data_engineer")
    assert not result.allowed
    assert result.reason_code == PII_BLOCKED


@pytest.mark.parametrize("column", ["player_email", "player_ip", "country", "birth_year"])
def test_every_pii_column_is_blocked(column):
    result = guard_sql(f"SELECT {column} FROM silver.sessions", "data_engineer")
    assert not result.allowed
    assert result.reason_code == PII_BLOCKED


def test_pii_in_a_where_clause_is_also_blocked():
    result = guard_sql(
        "SELECT session_id FROM silver.sessions WHERE birth_year > 2000", "data_engineer"
    )
    assert not result.allowed
    assert result.reason_code == PII_BLOCKED


def test_masked_views_are_exempt_from_the_pii_check():
    """The mask *is* the control, so reading a governed view is allowed."""
    result = guard_sql("SELECT player_email FROM secure.v_dim_player_analyst", ROLE)
    assert result.allowed


# ---------------------------------------------------------------------------
# guard: LIMIT injection
# ---------------------------------------------------------------------------


def test_default_limit_is_injected():
    result = guard_sql("SELECT * FROM secure.v_monetization_daily", ROLE)
    assert result.allowed
    assert result.limit_injected
    assert result.sql.upper().endswith(f"LIMIT {DEFAULT_LIMIT}")


def test_explicit_limit_is_preserved():
    result = guard_sql("SELECT * FROM secure.v_monetization_daily LIMIT 7", ROLE)
    assert result.allowed
    assert not result.limit_injected
    assert result.sql.upper().endswith("LIMIT 7")


def test_limit_is_clamped_to_the_maximum():
    result = guard_sql("SELECT * FROM secure.v_monetization_daily", ROLE, limit=10**9)
    assert "LIMIT 1000" in result.sql.upper()


def test_comments_are_stripped_before_validation():
    result = guard_sql(
        "SELECT /* sneaky */ * FROM secure.v_monetization_daily -- trailing",
        ROLE,
    )
    assert result.allowed
    assert "sneaky" not in result.sql


# ---------------------------------------------------------------------------
# tools
# ---------------------------------------------------------------------------


def test_tool_registry_matches_the_spec():
    assert set(TOOL_NAMES) == {
        "list_tables",
        "describe_table",
        "run_sql",
        "get_metric",
        "retention_cohort",
        "check_data_freshness",
    }


def test_list_tables_only_returns_granted_objects(spark, lakehouse):
    result = execute_tool(spark, "list_tables", {}, ROLE)
    assert result.status == "ok"
    assert result.row_count == 4
    assert all(row["table"].startswith("secure.") for row in result.rows)


def test_describe_table_reports_classification(spark, lakehouse):
    result = execute_tool(spark, "describe_table", {"table": "secure.v_monetization_daily"}, ROLE)
    assert result.status == "ok"
    assert {row["column"] for row in result.rows} >= {"dau", "revenue_usd", "arpdau"}


def test_describe_table_refuses_ungranted(spark, lakehouse):
    result = execute_tool(spark, "describe_table", {"table": "gold.dim_player"}, ROLE)
    assert result.status == "refused"
    assert result.reason_code == UNGRANTED_OBJECT


def test_run_sql_returns_rows_and_respects_limit(spark, lakehouse):
    result = execute_tool(
        spark, "run_sql", {"sql": "SELECT * FROM secure.v_monetization_daily"}, ROLE
    )
    assert result.status == "ok"
    assert result.row_count <= DEFAULT_LIMIT


def test_get_metric_returns_a_series(spark, lakehouse):
    result = execute_tool(spark, "get_metric", {"metric": "dau"}, ROLE)
    assert result.status == "ok"
    assert result.row_count > 0
    assert "dau" in result.rows[0]


def test_get_metric_rejects_an_unknown_metric(spark, lakehouse):
    result = execute_tool(spark, "get_metric", {"metric": "wibble"}, ROLE)
    assert result.status == "refused"
    assert result.reason_code == "AMBIGUOUS"


def test_get_metric_applies_a_date_range(spark, lakehouse):
    result = execute_tool(
        spark,
        "get_metric",
        {"metric": "dau", "date_range": ["2025-09-01", "2025-09-03"]},
        ROLE,
    )
    assert result.status == "ok"
    assert result.row_count == 3


def test_retention_cohort_tool(spark, lakehouse):
    result = execute_tool(spark, "retention_cohort", {"horizon": "d1"}, ROLE)
    assert result.status == "ok"
    assert result.row_count > 0
    assert "d1_retention" in result.rows[0]


def test_check_data_freshness_tool(spark, lakehouse):
    result = execute_tool(
        spark, "check_data_freshness", {"table": "secure.v_monetization_daily"}, ROLE
    )
    assert result.status == "ok"
    assert result.rows[0]["column"] == "event_date"


def test_check_data_freshness_refuses_ungranted(spark, lakehouse):
    result = execute_tool(spark, "check_data_freshness", {"table": "bronze.sessions"}, ROLE)
    assert result.status == "refused"
    assert result.reason_code == UNGRANTED_OBJECT


def test_unknown_tool_and_bad_arguments_are_refused(spark, lakehouse):
    assert execute_tool(spark, "nope", {}, ROLE).status == "refused"
    assert execute_tool(spark, "get_metric", {"metric": 5}, ROLE).status in {"ok", "refused"}


def test_tool_input_models_reject_bad_payloads():
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        RunSqlInput(sql=123)
    with pytest.raises(ValidationError):
        DescribeTableInput()
    # valid models
    assert ListTablesInput().layer is None
    assert GetMetricInput(metric="dau").date_range is None
    assert RetentionCohortInput().horizon == "all"
    assert FreshnessInput(table="secure.v_monetization_daily").table.startswith("secure.")
