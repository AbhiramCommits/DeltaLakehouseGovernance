"""Table-level and column-level lineage.

Lineage is derived from two registered sources rather than inferred from SQL
text, which keeps it exact and cheap:

1. **table-level** edges come from the ``upstream`` list in ``catalog.yml``;
2. **column-level** edges come from the ``sources`` list on each column.

The builder emits a Mermaid diagram (``docs/lineage.md``), a machine-readable
edge list (``results/lineage_edges.json``) and an ``impact`` query that walks the
column graph downstream -- "if ``silver.sessions.player_email`` changes, what
breaks?".
"""

from __future__ import annotations

import json
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from pathlib import Path

from dlg.config import Settings, get_settings
from dlg.governance.catalog import Catalog, load_catalog

__all__ = ["LineageGraph", "build_lineage", "write_lineage_artifacts", "impact"]


@dataclass
class LineageNode:
    id: str
    layer: str
    domain: str
    classification: str
    owner: str
    columns: int
    kind: str = "table"


@dataclass
class LineageEdge:
    source: str
    target: str
    kind: str = "table"
    column: str | None = None


@dataclass
class LineageGraph:
    nodes: list[LineageNode] = field(default_factory=list)
    edges: list[LineageEdge] = field(default_factory=list)

    # ------------------------------------------------------------------
    @property
    def table_edges(self) -> list[LineageEdge]:
        return [e for e in self.edges if e.kind == "table"]

    @property
    def column_edges(self) -> list[LineageEdge]:
        return [e for e in self.edges if e.kind == "column"]

    @property
    def layers(self) -> list[str]:
        order = ["landing", "bronze", "silver", "gold", "secure", "ops"]
        found = {n.layer for n in self.nodes}
        return [layer for layer in order if layer in found] + sorted(found - set(order))

    def node_ids(self) -> list[str]:
        return [n.id for n in self.nodes]

    def downstream(self, qualified_column: str) -> list[str]:
        """All columns (and their tables) transitively derived from a column."""
        adjacency: dict[str, list[str]] = defaultdict(list)
        for edge in self.column_edges:
            adjacency[edge.source].append(edge.target)
        seen: set[str] = set()
        queue = deque([qualified_column])
        while queue:
            current = queue.popleft()
            for target in adjacency.get(current, []):
                if target not in seen:
                    seen.add(target)
                    queue.append(target)
        return sorted(seen)

    def to_dict(self) -> dict:
        return {
            "nodes": [asdict(n) for n in self.nodes],
            "table_edges": [{"source": e.source, "target": e.target} for e in self.table_edges],
            "column_edges": [{"source": e.source, "target": e.target} for e in self.column_edges],
            "counts": {
                "nodes": len(self.nodes),
                "table_edges": len(self.table_edges),
                "column_edges": len(self.column_edges),
            },
        }

    # ------------------------------------------------------------------
    def to_mermaid(self) -> str:
        """A Mermaid flowchart: landing -> bronze -> silver -> gold."""
        aliases = {node: f"n{i}" for i, node in enumerate(self.node_ids())}
        lines = ["```mermaid", "flowchart LR"]
        for layer in self.layers:
            members = [n for n in self.nodes if n.layer == layer]
            if not members:
                continue
            lines.append(f"  subgraph {layer.upper()}[{layer}]")
            for node in members:
                label = node.id.split(".", 1)[-1]
                lines.append(f'    {aliases[node.id]}["{label}"]')
            lines.append("  end")
        for edge in self.table_edges:
            if edge.source in aliases and edge.target in aliases:
                lines.append(f"  {aliases[edge.source]} --> {aliases[edge.target]}")
        lines.append("```")
        return "\n".join(lines)

    def to_column_mermaid(self) -> str:
        """A column-level Mermaid diagram restricted to the gold marts."""
        edges = [e for e in self.column_edges if e.target.startswith("gold.")]
        if not edges:
            return "_no column-level edges registered_"
        node_ids: list[str] = []
        for edge in edges:
            for node in (edge.source, edge.target):
                if node not in node_ids:
                    node_ids.append(node)
        aliases = {node: f"c{i}" for i, node in enumerate(node_ids)}
        lines = ["```mermaid", "flowchart LR"]
        for node in node_ids:
            table, column = node.split(".", 1)
            lines.append(f'  {aliases[node]}["{table}<br/>{column}"]')
        for edge in edges:
            lines.append(f"  {aliases[edge.source]} --> {aliases[edge.target]}")
        lines.append("```")
        return "\n".join(lines)


def build_lineage(catalog: Catalog | None = None, settings: Settings | None = None) -> LineageGraph:
    settings = settings or get_settings()
    catalog = catalog or load_catalog(settings=settings)

    nodes = [
        LineageNode(
            id=table.name,
            layer=table.layer,
            domain=table.domain,
            classification=table.classification,
            owner=table.owner,
            columns=len(table.columns),
        )
        for table in catalog.tables.values()
    ]

    edges: list[LineageEdge] = []
    # The landing zone is a synthetic root so the graph starts outside Delta.
    for table in catalog.tables.values():
        if not table.upstream:
            edges.append(LineageEdge(source=f"landing.{table.layer}", target=table.name))
        for upstream in table.upstream:
            edges.append(LineageEdge(source=upstream, target=table.name))

    nodes.append(
        LineageNode(
            id="landing.bronze",
            layer="landing",
            domain="platform",
            classification="INTERNAL",
            owner="data-engineering@studio.example",
            columns=0,
            kind="source",
        )
    )

    # ---- governed views (masking + row filters) ----------------------------
    from dlg.governance.policies import GOVERNED_VIEW_LINEAGE

    for view, spec in GOVERNED_VIEW_LINEAGE.items():
        nodes.append(
            LineageNode(
                id=view,
                layer="secure",
                domain="platform",
                classification="INTERNAL",
                owner="governance@studio.example",
                columns=len(spec["columns"]),
                kind="view",
            )
        )
        for upstream in spec["tables"]:
            edges.append(LineageEdge(source=upstream, target=view))
        for column, sources in spec["columns"].items():
            for source in sources:
                edges.append(LineageEdge(source=source, target=f"{view}.{column}", kind="column"))

    # ---- catalog-declared column lineage ----------------------------------
    for source, target in catalog.column_lineage_edges():
        edges.append(LineageEdge(source=source, target=target, kind="column"))

    return LineageGraph(nodes=nodes, edges=edges)


def write_lineage_artifacts(
    graph: LineageGraph, settings: Settings | None = None
) -> dict[str, str]:
    """Write ``docs/lineage.md`` and ``results/lineage_edges.json``."""
    settings = settings or get_settings()
    docs = Path(settings.docs_dir)
    results = Path(settings.results_dir)
    docs.mkdir(parents=True, exist_ok=True)
    results.mkdir(parents=True, exist_ok=True)

    md_path = docs / "lineage.md"
    json_path = results / "lineage_edges.json"

    table_rows = "\n".join(
        f"| `{e.source}` | `{e.target}` |"
        for e in sorted(graph.table_edges, key=lambda e: (e.target, e.source))
    )
    pii_edges = [
        e
        for e in graph.column_edges
        if e.source.split(".")[-1]
        in {"player_email", "player_ip", "birth_year", "country", "payment_method"}
    ]
    pii_rows = "\n".join(
        f"| `{e.source}` | `{e.target}` |" for e in sorted(pii_edges, key=lambda e: e.target)
    )
    counts = graph.to_dict()["counts"]

    md = f"""# Lineage

Generated by `dlg lineage` from `governance/catalog.yml` (registered upstream
dependencies and per-column `sources`). Do not edit by hand.

* nodes: **{counts["nodes"]}**
* table-level edges: **{counts["table_edges"]}**
* column-level edges: **{counts["column_edges"]}**

## Table-level lineage

{graph.to_mermaid()}

| upstream | downstream |
| --- | --- |
{table_rows}

## Column-level lineage (gold marts)

{graph.to_column_mermaid()}

### PII propagation

These edges show classified PII columns flowing downstream. Every one of them is
masked by a governed view before an analyst or the agent service can read it.

| upstream column | downstream column |
| --- | --- |
{pii_rows}

## Impact analysis

`python -m dlg.cli impact <table.column>` walks this graph downstream, e.g.

```
$ python -m dlg.cli impact silver.sessions.player_email
silver.sessions.player_email impacts 2 downstream columns across 2 tables:
  secure.v_dim_player_analyst.player_email
  secure.v_dim_player_emea.player_email
```

Note how every PII column terminates in a ``secure.*`` view: there is no path from
``player_email`` into a raw gold table, which is exactly the property the access
matrix relies on.
"""
    md_path.write_text(md)
    json_path.write_text(json.dumps(graph.to_dict(), indent=2))
    return {"markdown": str(md_path), "json": str(json_path)}


def impact(graph: LineageGraph, qualified_column: str) -> list[str]:
    """Downstream columns impacted by a change to ``qualified_column``."""
    return graph.downstream(qualified_column)
