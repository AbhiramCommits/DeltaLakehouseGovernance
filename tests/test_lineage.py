"""Lineage graph, impact analysis and the emitted artefacts."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dlg.governance.lineage import build_lineage, impact, write_lineage_artifacts


@pytest.fixture(scope="module")
def graph():
    return build_lineage()


def test_graph_has_nodes_and_edges(graph):
    assert len(graph.nodes) >= 19
    assert len(graph.table_edges) >= 20
    assert len(graph.column_edges) >= 100
    assert graph.layers[0] == "landing"
    assert "silver" in graph.layers
    assert "gold" in graph.layers
    assert "secure" in graph.layers


def test_every_node_has_an_owner_and_a_layer(graph):
    for node in graph.nodes:
        assert node.layer
        assert node.owner


def test_table_lineage_flows_bronze_to_silver_to_gold(graph):
    edges = {(e.source, e.target) for e in graph.table_edges}
    assert ("bronze.sessions", "silver.sessions") in edges
    assert ("silver.sessions", "gold.fct_player_daily") in edges
    assert ("silver.sessions", "gold.fct_retention_cohort") in edges


def test_governed_views_appear_in_the_graph(graph):
    ids = set(graph.node_ids())
    assert "secure.v_dim_player_analyst" in ids
    assert "secure.v_monetization_daily" in ids
    edges = {(e.source, e.target) for e in graph.table_edges}
    assert ("gold.dim_player", "secure.v_dim_player_analyst") in edges


def test_pii_impact_terminates_in_a_secure_view(graph):
    downstream = impact(graph, "silver.sessions.player_email")
    assert downstream, "PII columns must have registered downstream consumers"
    assert all(target.startswith("secure.") for target in downstream)


def test_playtime_impact_reaches_both_the_mart_and_the_view(graph):
    downstream = impact(graph, "silver.sessions.duration_minutes")
    assert "gold.fct_player_daily.playtime_minutes" in downstream
    assert "gold.dim_player.lifetime_playtime_minutes" in downstream
    assert "secure.v_dim_player_analyst.lifetime_playtime_minutes" in downstream


def test_single_hop_impact_is_traced(graph):
    """A column that is only carried forward still shows up downstream."""
    assert impact(graph, "bronze.purchases.sku") == ["silver.purchases.sku"]


def test_impact_on_a_leaf_column_is_empty(graph):
    """Nothing downstream consumes a column that no mart or view reads."""
    assert impact(graph, "silver.sessions.app_version") == []


def test_column_edges_are_qualified(graph):
    for edge in graph.column_edges:
        assert "." in edge.source and "." in edge.target
        assert not edge.source.startswith("landing.")


def test_mermaid_rendering(graph):
    mermaid = graph.to_mermaid()
    assert mermaid.startswith("```mermaid")
    assert mermaid.rstrip().endswith("```")
    assert "flowchart LR" in mermaid
    assert "subgraph SILVER" in mermaid


def test_artifacts_are_written(graph, tmp_path, monkeypatch):
    settings = type(
        "S", (), {"docs_dir": str(tmp_path / "docs"), "results_dir": str(tmp_path / "results")}
    )()
    paths = write_lineage_artifacts(graph, settings=settings)
    markdown = Path(paths["markdown"]).read_text()
    payload = json.loads(Path(paths["json"]).read_text())

    assert "# Lineage" in markdown
    assert "mermaid" in markdown
    assert payload["counts"]["nodes"] == len(graph.nodes)
    assert len(payload["table_edges"]) == len(graph.table_edges)
    assert len(payload["column_edges"]) == len(graph.column_edges)


def test_lineage_is_deterministic():
    first = build_lineage().to_dict()["counts"]
    second = build_lineage().to_dict()["counts"]
    assert first == second
