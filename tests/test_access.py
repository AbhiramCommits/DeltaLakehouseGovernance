"""The role-to-grant matrix and its invariants."""

from __future__ import annotations

import pytest

from dlg.governance.access import (
    ALL_OPERATIONS,
    REASON_ALLOW,
    REASON_NO_GRANT,
    REASON_READ_ONLY,
    REASON_SCOPE,
    REASON_UNKNOWN_ROLE,
    check_access,
    grant_totals,
    load_roles,
)
from dlg.governance.policies import GOVERNED_VIEWS

SECURE_OBJECTS = [f"secure.{v}" for v in GOVERNED_VIEWS]
RAW_OBJECTS = [
    "bronze.sessions",
    "silver.sessions",
    "silver.purchases",
    "gold.dim_player",
    "ops.agent_audit",
]


def test_all_expected_roles_are_defined():
    matrix = load_roles()
    assert set(matrix.role_names) == {
        "admin",
        "data_engineer",
        "analyst",
        "regional_analyst_emea",
        "marketing",
        "agent_service",
    }


@pytest.mark.parametrize("obj", RAW_OBJECTS)
def test_agent_service_is_denied_on_every_raw_object(obj):
    decision = check_access("agent_service", obj, "SELECT")
    assert not decision.allowed
    assert decision.reason_code == REASON_SCOPE


@pytest.mark.parametrize("obj", ["secure.v_dim_player_analyst", "secure.v_monetization_daily"])
def test_agent_service_is_allowed_on_governed_views(obj):
    decision = check_access("agent_service", obj, "SELECT")
    assert decision.allowed
    assert decision.reason_code == REASON_ALLOW


@pytest.mark.parametrize(
    "operation", ["INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "DROP", "ALTER"]
)
def test_agent_service_cannot_write_anything(operation):
    decision = check_access("agent_service", "secure.v_monetization_daily", operation)
    assert not decision.allowed


def test_analyst_cannot_read_raw_tables_but_can_read_views():
    assert not check_access("analyst", "silver.sessions", "SELECT").allowed
    assert check_access("analyst", "secure.v_dim_player_analyst", "SELECT").allowed


def test_marketing_cannot_read_the_analyst_only_view():
    decision = check_access("marketing", "secure.v_fct_player_daily", "SELECT")
    assert not decision.allowed
    assert decision.reason_code == REASON_NO_GRANT


def test_regional_analyst_is_scoped_to_emea_views():
    assert check_access("regional_analyst_emea", "secure.v_dim_player_emea", "SELECT").allowed
    assert not check_access(
        "regional_analyst_emea", "secure.v_dim_player_analyst", "SELECT"
    ).allowed


def test_data_engineer_can_write_bronze_and_silver():
    assert check_access("data_engineer", "bronze.sessions", "MERGE").allowed
    assert check_access("data_engineer", "silver.sessions", "UPDATE").allowed


def test_unknown_role_is_denied_with_a_reason():
    decision = check_access("intern", "secure.v_monetization_daily", "SELECT")
    assert not decision.allowed
    assert decision.reason_code == REASON_UNKNOWN_ROLE


def test_operation_not_granted_has_its_own_reason_code():
    decision = check_access("analyst", "secure.v_dim_player_analyst", "DELETE")
    assert not decision.allowed
    assert decision.reason_code == REASON_READ_ONLY


def test_admin_can_do_everything():
    for operation in ALL_OPERATIONS:
        assert check_access("admin", "gold.dim_player", operation).allowed, operation


def test_grant_matrix_totals_are_consistent():
    objects = RAW_OBJECTS + SECURE_OBJECTS
    totals = grant_totals(objects)
    assert totals["total_cells"] == len(objects) * 6 * len(ALL_OPERATIONS)
    assert totals["allow_cells"] + totals["deny_cells"] == totals["total_cells"]
    assert 0 < totals["allow_cells"] < totals["total_cells"]


def test_roles_file_rejects_an_out_of_scope_agent_grant(tmp_path):
    bad = tmp_path / "roles.yml"
    bad.write_text(
        """
version: 1
roles:
  agent_service:
    grants:
      - { object: "bronze.sessions", operations: [SELECT] }
"""
    )
    with pytest.raises(ValueError) as exc:
        load_roles(bad)
    assert "secure." in str(exc.value)


def test_roles_file_rejects_a_write_grant_for_a_read_only_role(tmp_path):
    bad = tmp_path / "roles.yml"
    bad.write_text(
        """
version: 1
roles:
  analyst:
    grants:
      - { object: "secure.v_monetization_daily", operations: [SELECT, DELETE] }
"""
    )
    with pytest.raises(ValueError):
        load_roles(bad)


def test_agent_service_grant_count_is_exactly_four():
    matrix = load_roles()
    grants = matrix.grants("agent_service")
    assert len(grants) == 4
    assert all(g.object.startswith("secure.") for g in grants)
