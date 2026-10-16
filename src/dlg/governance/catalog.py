"""Catalog loading and drift detection.

``governance/catalog.yml`` declares every table, column, classification, owner,
SLA and lineage edge.  This module loads it, validates it against the *live*
Delta schemas, and fails loudly when the two disagree -- in either direction:

* a physical column that the catalog does not declare (undeclared schema drift),
* a catalog column that no longer exists physically (stale metadata),
* a type mismatch between the declaration and Delta.

Running this in CI is what stops an "innocent" ``ALTER TABLE ADD COLUMN`` from
silently shipping an undocumented, unclassified column.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml
from pyspark.sql import SparkSession

from dlg.config import Settings, get_settings

__all__ = [
    "Catalog",
    "CatalogTable",
    "ColumnSpec",
    "DriftIssue",
    "ValidationReport",
    "load_catalog",
    "validate_catalog",
    "CLASSIFICATIONS",
]

CLASSIFICATIONS = ("PUBLIC", "INTERNAL", "PII", "FINANCIAL")

#: catalog YAML type names -> Spark SQL type names (as reported by simpleString())
TYPE_ALIASES = {
    "string": "string",
    "int": "int",
    "integer": "int",
    "bigint": "bigint",
    "long": "bigint",
    "double": "double",
    "float": "float",
    "boolean": "boolean",
    "bool": "boolean",
    "timestamp": "timestamp",
    "date": "date",
}


@dataclass
class ColumnSpec:
    name: str
    type: str
    description: str
    classification: str
    keys: list[str] = field(default_factory=list)
    sources: list[str] = field(default_factory=list)

    @property
    def is_pii(self) -> bool:
        return self.classification == "PII"

    @property
    def qualified(self) -> str:
        return self.name


@dataclass
class SlaBlock:
    max_staleness_minutes: int = 43_200
    min_daily_rows: int = 0
    max_null_rate: float = 0.05

    @classmethod
    def from_dict(cls, data: dict | None) -> SlaBlock:
        data = data or {}
        return cls(
            max_staleness_minutes=int(data.get("max_staleness_minutes", 43_200)),
            min_daily_rows=int(data.get("min_daily_rows", 0)),
            max_null_rate=float(data.get("max_null_rate", 0.05)),
        )


@dataclass
class CatalogTable:
    name: str
    owner: str
    domain: str
    layer: str
    description: str
    classification: str
    upstream: list[str]
    sla: SlaBlock
    columns: dict[str, ColumnSpec]

    @property
    def pii_columns(self) -> list[str]:
        return [c.name for c in self.columns.values() if c.is_pii]

    @property
    def financial_columns(self) -> list[str]:
        return [c.name for c in self.columns.values() if c.classification == "FINANCIAL"]

    def column(self, name: str) -> ColumnSpec | None:
        return self.columns.get(name)


@dataclass
class Catalog:
    version: int
    name: str
    domains: dict[str, str]
    tables: dict[str, CatalogTable]

    def __contains__(self, table: str) -> bool:
        return table in self.tables

    def __getitem__(self, table: str) -> CatalogTable:
        return self.tables[table]

    @property
    def pii_columns(self) -> list[str]:
        out: list[str] = []
        for table in self.tables.values():
            out.extend(f"{table.name}.{c}" for c in table.pii_columns)
        return out

    def column_lineage_edges(self) -> list[tuple[str, str]]:
        """``(upstream_table.column, downstream_table.column)`` pairs."""
        edges: list[tuple[str, str]] = []
        for table in self.tables.values():
            for column in table.columns.values():
                for source in column.sources:
                    edges.append((source, f"{table.name}.{column.name}"))
        return edges

    def table_lineage_edges(self) -> list[tuple[str, str]]:
        edges: list[tuple[str, str]] = []
        for table in self.tables.values():
            for upstream in table.upstream:
                edges.append((upstream, table.name))
        return edges


@dataclass
class DriftIssue:
    table: str
    kind: str  # UNDECLARED_COLUMN | MISSING_COLUMN | TYPE_MISMATCH | MISSING_TABLE | BAD_CLASSIFICATION
    column: str = ""
    detail: str = ""

    def __str__(self) -> str:  # pragma: no cover - formatting only
        target = f"{self.table}.{self.column}" if self.column else self.table
        return f"[{self.kind}] {target}: {self.detail}"


@dataclass
class ValidationReport:
    checked_tables: list[str] = field(default_factory=list)
    missing_tables: list[str] = field(default_factory=list)
    issues: list[DriftIssue] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.issues

    def render(self) -> str:
        lines = [
            f"catalog validation: {len(self.checked_tables)} tables checked, "
            f"{len(self.issues)} drift issue(s)"
        ]
        if self.missing_tables:
            lines.append(f"  missing physical tables: {', '.join(self.missing_tables)}")
        for issue in self.issues:
            lines.append(f"  {issue}")
        if self.ok:
            lines.append("  OK — catalog matches the live Delta schemas exactly")
        return "\n".join(lines)


def load_catalog(path: str | Path | None = None, settings: Settings | None = None) -> Catalog:
    settings = settings or get_settings()
    catalog_path = Path(path or settings.catalog_path)
    if not catalog_path.exists():
        raise FileNotFoundError(f"catalog file not found: {catalog_path}")
    raw = yaml.safe_load(catalog_path.read_text())

    tables: dict[str, CatalogTable] = {}
    for name, spec in (raw.get("tables") or {}).items():
        columns: dict[str, ColumnSpec] = {}
        for column_name, column in (spec.get("columns") or {}).items():
            classification = str(column.get("classification", "INTERNAL")).upper()
            if classification not in CLASSIFICATIONS:
                raise ValueError(
                    f"{name}.{column_name}: unknown classification {classification!r}; "
                    f"expected one of {CLASSIFICATIONS}"
                )
            columns[column_name] = ColumnSpec(
                name=column_name,
                type=str(column.get("type", "")).lower(),
                description=str(column.get("description", "")),
                classification=classification,
                keys=list(column.get("keys") or []),
                sources=list(column.get("sources") or []),
            )
        tables[name] = CatalogTable(
            name=name,
            owner=spec.get("owner", "unowned"),
            domain=spec.get("domain", "unknown"),
            layer=spec.get("layer", name.split(".")[0]),
            description=str(spec.get("description", "")).strip(),
            classification=str(spec.get("classification", "INTERNAL")).upper(),
            upstream=list(spec.get("upstream") or []),
            sla=SlaBlock.from_dict(spec.get("sla")),
            columns=columns,
        )
    return Catalog(
        version=int(raw.get("version", 1)),
        name=str(raw.get("name", "lakehouse")),
        domains=dict(raw.get("domains") or {}),
        tables=tables,
    )


def validate_catalog(
    spark: SparkSession,
    catalog: Catalog | None = None,
    *,
    settings: Settings | None = None,
    require_all: bool = False,
) -> ValidationReport:
    """Compare the catalog against the live Delta schemas.

    Tables that do not physically exist are reported in ``missing_tables``; they
    only become hard errors when ``require_all`` is set (the DQ/agent layers add
    ``ops.*`` tables in later steps, and a partial lake is valid mid-build).
    """
    settings = settings or get_settings()
    catalog = catalog or load_catalog(settings=settings)
    report = ValidationReport()

    for name, table in catalog.tables.items():
        try:
            physical = {f.name: f.dataType.simpleString() for f in spark.table(name).schema.fields}
        except Exception:
            report.missing_tables.append(name)
            if require_all:
                report.issues.append(
                    DriftIssue(name, "MISSING_TABLE", detail="declared in the catalog but absent")
                )
            continue

        report.checked_tables.append(name)

        explained = set(table.columns)
        if table.classification not in CLASSIFICATIONS:
            report.issues.append(
                DriftIssue(
                    name,
                    "BAD_CLASSIFICATION",
                    detail=f"table classification {table.classification!r} is not valid",
                )
            )

        for column, spark_type in physical.items():
            if column not in explained:
                report.issues.append(
                    DriftIssue(
                        name,
                        "UNDECLARED_COLUMN",
                        column=column,
                        detail=(
                            f"physical column of type {spark_type} is not declared in the "
                            f"catalog (undocumented / unclassified drift)"
                        ),
                    )
                )
            else:
                declared = TYPE_ALIASES.get(table.columns[column].type, table.columns[column].type)
                if declared != spark_type:
                    report.issues.append(
                        DriftIssue(
                            name,
                            "TYPE_MISMATCH",
                            column=column,
                            detail=f"catalog says {declared!r}, Delta says {spark_type!r}",
                        )
                    )

        for column in table.columns:
            if column not in physical:
                report.issues.append(
                    DriftIssue(
                        name,
                        "MISSING_COLUMN",
                        column=column,
                        detail="declared in the catalog but absent from the physical table",
                    )
                )

    return report


def catalog_summary(catalog: Catalog) -> dict[str, int]:
    """Counts used in the results file."""
    columns = sum(len(t.columns) for t in catalog.tables.values())
    return {
        "tables": len(catalog.tables),
        "columns": columns,
        "pii_columns": len(catalog.pii_columns),
        "financial_columns": sum(len(t.financial_columns) for t in catalog.tables.values()),
        "table_lineage_edges": len(catalog.table_lineage_edges()),
        "column_lineage_edges": len(catalog.column_lineage_edges()),
        "domains": len(catalog.domains),
    }
