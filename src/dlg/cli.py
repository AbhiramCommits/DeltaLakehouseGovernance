"""Typer CLI for the Delta lakehouse.

Run ``python -m dlg.cli --help`` for the full command list, or use the
``make`` targets which wrap the common flows.
"""

from __future__ import annotations

import json
from pathlib import Path

import typer

from dlg.config import get_settings

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Delta Lakehouse Governance — medallion lakehouse, governance, quality and agent CLI.",
)


def _write_json(path: str | None, payload: object) -> None:
    if not path:
        return
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    if hasattr(payload, "as_dict"):
        payload = payload.as_dict()  # type: ignore[union-attr]
    payload = _jsonable(payload)
    target.write_text(json.dumps(payload, indent=2, default=str))
    typer.echo(f"wrote {target}")


def _jsonable(obj: object) -> object:
    from dataclasses import asdict, is_dataclass

    if hasattr(obj, "as_dict"):
        return _jsonable(obj.as_dict())  # type: ignore[union-attr]
    if is_dataclass(obj) and not isinstance(obj, type):
        return _jsonable(asdict(obj))
    if isinstance(obj, dict):
        return {str(k): _jsonable(v) for k, v in obj.items()}
    if isinstance(obj, list | tuple):
        return [_jsonable(v) for v in obj]
    return obj


# ======================================================================
# Step 1 — data generation
# ======================================================================


@app.command("gen-data")
def gen_data(
    seed: int = typer.Option(None, help="Override the RNG seed (default 42)."),
    scale: str = typer.Option(None, help="full | small (CI uses small)."),
    json_out: str = typer.Option(None, help="Also write the generator summary to this JSON path."),
) -> None:
    """Generate the synthetic landing zone with injected, counted defects."""
    from dlg.generate_data import main as gen_main

    argv: list[str] = []
    if seed is not None:
        argv += ["--seed", str(seed)]
    if scale is not None:
        argv += ["--scale", scale]
    if json_out:
        argv += ["--json-out", json_out]
    gen_main(argv)


@app.command("info")
def info() -> None:
    """Print the resolved configuration."""
    settings = get_settings()
    typer.echo(f"lake_root          = {settings.lake_root}")
    typer.echo(f"landing_root       = {settings.landing}")
    typer.echo(f"checkpoint_root    = {settings.checkpoints}")
    typer.echo(f"catalog_file       = {settings.catalog_path}")
    typer.echo(f"roles_file         = {settings.roles_path}")
    typer.echo(f"shuffle_partitions = {settings.shuffle_partitions}")
    typer.echo(f"scale              = {settings.scale} (factor {settings.scale_factor})")
    typer.echo(f"remote_lake        = {settings.is_remote_lake}")


# ======================================================================
# Step 2 — bronze, silver, time travel
# ======================================================================


@app.command("bronze")
def bronze_cmd(
    json_out: str = typer.Option(None, help="Write measured ingest stats as JSON."),
    skip_updates: bool = typer.Option(False, help="Skip the late-arriving revision batch."),
) -> None:
    """Ingest landing files into the bronze Delta tables (idempotent)."""
    from dlg.pipelines.bronze import print_report, run_bronze
    from dlg.spark import get_spark

    spark = get_spark("dlg-bronze")
    results = run_bronze(spark, include_updates=not skip_updates)
    print_report(results)
    _write_json(json_out, {k: v.as_dict() for k, v in results.items()})


@app.command("silver")
def silver_cmd(
    json_out: str = typer.Option(None, help="Write measured cleanse/MERGE stats as JSON."),
) -> None:
    """Cleanse, dedup and MERGE bronze data into the silver Delta tables."""
    from dlg.pipelines.silver import print_report, run_silver
    from dlg.spark import get_spark

    spark = get_spark("dlg-silver")
    results = run_silver(spark)
    print_report(results)
    _write_json(json_out, {k: v.as_dict() for k, v in results.items()})


@app.command("history")
def history_cmd(
    table: str = typer.Argument(..., help="Delta table, e.g. silver.sessions"),
    limit: int = typer.Option(20, help="How many commits to show."),
) -> None:
    """Print Delta history (version, operation, metrics) for a table."""
    from dlg.spark import get_spark
    from dlg.timetravel import print_history

    print_history(get_spark("dlg-history"), table, limit=limit)


@app.command("demo-rollback")
def demo_rollback_cmd(
    table: str = typer.Option("silver.purchases", help="Table to corrupt and restore."),
    json_out: str = typer.Option(None, help="Write the before/corrupt/after counts as JSON."),
) -> None:
    """Corrupt a table with a bad MERGE, then RESTORE it and assert the counts."""
    from dlg.spark import get_spark
    from dlg.timetravel import demo_rollback

    report = demo_rollback(get_spark("dlg-rollback"), table)
    _write_json(json_out, report)


# ======================================================================
# Step 3 — gold marts + governance
# ======================================================================


@app.command("gold")
def gold_cmd(
    json_out: str = typer.Option(None, help="Write gold mart stats + headline metrics as JSON."),
) -> None:
    """Build the gold marts from the silver layer."""
    from dlg.pipelines.gold import build_all, headline_metrics, print_report
    from dlg.spark import get_spark

    spark = get_spark("dlg-gold")
    result = build_all(spark)
    print_report(result)
    payload = result.as_dict()
    payload["headline_metrics"] = headline_metrics(spark)
    _write_json(json_out, payload)


@app.command("catalog-validate")
def catalog_validate_cmd(
    require_all: bool = typer.Option(
        False, help="Fail if a catalog table is missing from the lake."
    ),
    json_out: str = typer.Option(None, help="Write the validation summary as JSON."),
) -> None:
    """Validate governance/catalog.yml against the live Delta schemas (drift detection)."""
    from dlg.governance.catalog import catalog_summary, load_catalog, validate_catalog
    from dlg.spark import get_spark

    catalog = load_catalog()
    report = validate_catalog(get_spark("dlg-catalog"), catalog, require_all=require_all)
    typer.echo(report.render())
    summary = catalog_summary(catalog)
    summary.update(
        {
            "checked_tables": len(report.checked_tables),
            "missing_tables": report.missing_tables,
            "drift_issues": [str(i) for i in report.issues],
            "ok": report.ok,
        }
    )
    _write_json(json_out, summary)
    if not report.ok:
        raise typer.Exit(code=1)


@app.command("lineage")
def lineage_cmd(
    json_out: str = typer.Option(None, help="Write lineage counts as JSON."),
) -> None:
    """Build table + column lineage and emit docs/lineage.md and results/lineage_edges.json."""
    from dlg.governance.lineage import build_lineage, write_lineage_artifacts

    graph = build_lineage()
    paths = write_lineage_artifacts(graph)
    payload = graph.to_dict()["counts"]
    payload["artifacts"] = paths
    for name, path in paths.items():
        typer.echo(f"wrote {name}: {path}")
    typer.echo(
        f"lineage: {payload['nodes']} nodes, {payload['table_edges']} table edges, "
        f"{payload['column_edges']} column edges"
    )
    _write_json(json_out, payload)


@app.command("impact")
def impact_cmd(
    column: str = typer.Argument(..., help="Qualified column, e.g. silver.sessions.player_email"),
) -> None:
    """Print everything downstream of a column (change impact analysis)."""
    from dlg.governance.lineage import build_lineage, impact

    graph = build_lineage()
    downstream = impact(graph, column)
    if not downstream:
        typer.echo(f"no registered downstream columns depend on {column}")
        return
    tables = sorted({d.rsplit(".", 1)[0] for d in downstream})
    typer.echo(
        f"{column} impacts {len(downstream)} downstream columns across {len(tables)} tables:"
    )
    for column_name in downstream:
        typer.echo(f"  {column_name}")


@app.command("policies-apply")
def policies_apply_cmd(
    json_out: str = typer.Option(None, help="Write the policy report as JSON."),
) -> None:
    """Create the governed secure.* views (PII masking + region row filters)."""
    from dlg.governance.policies import apply_policies, masking_proof, region_filter_proof
    from dlg.spark import get_spark

    spark = get_spark("dlg-policies")
    report = apply_policies(spark)
    payload = report.as_dict()
    payload["masking_proof"] = masking_proof(spark)
    payload["region_filter_proof"] = region_filter_proof(spark)
    for view in report.views_created:
        typer.echo(f"created view {view}")
    typer.echo(
        f"\nmasked PII columns ({len(report.masked_pii_columns)}): "
        + ", ".join(report.masked_pii_columns)
    )
    typer.echo(f"row-filtered views: {', '.join(report.row_filtered_views)}")
    typer.echo(f"age buckets anchored on reference year {report.reference_year}")
    typer.echo("\nraw vs governed (same player):")
    typer.echo(f"  raw      : {payload['masking_proof']['raw']}")
    typer.echo(f"  governed : {payload['masking_proof']['governed']}")
    typer.echo(f"  row filter: {payload['region_filter_proof']}")
    _write_json(json_out, payload)


@app.command("access-matrix")
def access_matrix_cmd(
    operation: str = typer.Option("SELECT", help="Operation to display in the grid."),
    json_out: str = typer.Option(None, help="Write the full allow/deny cell counts as JSON."),
) -> None:
    """Print the role-to-grant matrix and the allow/deny cell counts."""
    from tabulate import tabulate

    from dlg.governance.access import ALL_OPERATIONS, grant_totals, load_roles, matrix_table
    from dlg.governance.catalog import load_catalog
    from dlg.governance.policies import GOVERNED_VIEWS

    catalog = load_catalog()
    objects = list(catalog.tables) + [f"secure.{v}" for v in GOVERNED_VIEWS]
    matrix = load_roles()

    rows = matrix_table(objects, operation=operation)
    role_names = matrix.role_names
    # transpose: one row per object, one column per role
    grid = [
        [obj] + [rows[j][i + 1] for j in range(len(role_names))] for i, obj in enumerate(objects)
    ]
    headers = ["object"] + role_names
    typer.echo(f"# Access matrix — operation = {operation}")
    typer.echo(tabulate(grid, headers=headers, tablefmt="github"))

    totals = grant_totals(objects)
    typer.echo(
        f"\nfull grid ({len(matrix.role_names)} roles x {len(objects)} objects x "
        f"{len(ALL_OPERATIONS)} operations = {totals['total_cells']} cells): "
        f"{totals['allow_cells']} allow, {totals['deny_cells']} deny"
    )
    typer.echo("\nroles:")
    for role in matrix.role_names:
        grants = matrix.grants(role)
        typer.echo(f"  {role}: {len(grants)} grant(s) — {matrix.description(role).splitlines()[0]}")

    # spot-check the headline guarantee
    from dlg.governance.access import check_access

    for obj in (
        "bronze.sessions",
        "silver.sessions",
        "gold.dim_player",
        "secure.v_dim_player_analyst",
    ):
        decision = check_access("agent_service", obj, "SELECT")
        typer.echo(
            f"  agent_service SELECT {obj}: {'allow' if decision.allowed else 'deny'} ({decision.reason_code})"
        )

    payload = dict(totals)
    payload["objects"] = objects
    payload["roles"] = matrix.role_names
    payload["operations"] = list(ALL_OPERATIONS)
    _write_json(json_out, payload)


def main() -> None:
    app()


# ======================================================================
# Step 4 — data quality, SLAs and observability
# ======================================================================


@app.command("quality-run")
def quality_run_cmd(
    suites: str = typer.Option(None, help="Path to a suites.yml (defaults to the packaged one)."),
    json_out: str = typer.Option(None, help="Write the measured DQ summary as JSON."),
) -> None:
    """Run the data-quality expectation suites; exit non-zero on a BLOCKING failure."""
    from dlg.quality.alerts import alerts_path, build_alerts, dispatch_alerts
    from dlg.quality.runner import print_report, run_quality
    from dlg.spark import get_spark

    spark = get_spark("dlg-quality")
    run = run_quality(spark, suites_path=suites)
    print_report(run)

    if run.failed:
        dispatch_alerts(build_alerts(quality=run))
        typer.echo(f"wrote {alerts_path()}")

    _write_json(json_out, run.summary())
    if run.blocking_failures:
        typer.echo(
            f"\n{len(run.blocking_failures)} BLOCKING expectation failure(s) — failing the run",
            err=True,
        )
        raise typer.Exit(code=1)


@app.command("sla-check")
def sla_check_cmd(
    json_out: str = typer.Option(None, help="Write the measured SLA summary as JSON."),
) -> None:
    """Evaluate the catalog SLA blocks against the live tables."""
    from dlg.quality.alerts import alerts_path, build_alerts, dispatch_alerts
    from dlg.quality.slas import print_report, run_sla_checks
    from dlg.spark import get_spark

    spark = get_spark("dlg-sla")
    run = run_sla_checks(spark)
    print_report(run)

    if run.breaches:
        dispatch_alerts(build_alerts(sla=run))
        typer.echo(f"wrote {alerts_path()}")

    summary = run.summary()
    summary["breaches"] = [b.as_dict() for b in run.breaches]
    _write_json(json_out, summary)


@app.command("observe")
def observe_cmd(
    json_out: str = typer.Option(None, help="Write the observability snapshot summary as JSON."),
) -> None:
    """Snapshot per-table metrics into ops.table_metrics and write the report."""
    from dlg.quality.observability import observe, write_report
    from dlg.spark import get_spark

    spark = get_spark("dlg-observe")
    snapshot = observe(spark, run_id="observe")
    report = write_report(snapshot)
    from tabulate import tabulate

    print(
        tabulate(
            [
                [m.table, f"{m.rows:,}", f"{m.size_bytes / 1e6:.2f}", m.num_files, m.delta_version]
                for m in snapshot.tables
            ],
            headers=["table", "rows", "size_mb", "files", "delta_version"],
            tablefmt="github",
        )
    )
    typer.echo(f"wrote {report}")
    _write_json(json_out, snapshot.summary())


# ======================================================================
# Step 5 — incremental Structured Streaming ingest
# ======================================================================


@app.command("stream")
def stream_cmd(
    once: bool = typer.Option(False, "--once", help="Run with the availableNow trigger and exit."),
    continuous: bool = typer.Option(False, "--continuous", help="Run continuously."),
    processing_time: str = typer.Option("10 seconds", help="Trigger interval for --continuous."),
    replay_last_batch: bool = typer.Option(
        False,
        "--replay-last-batch",
        help="Re-deliver the most recent micro-batch (expects zero rows).",
    ),
    json_out: str = typer.Option(None, help="Write the per-batch stats as JSON."),
) -> None:
    """Run the incremental streaming ingest over the landing directory."""
    from dlg.pipelines.stream import (
        print_report,
        run_stream,
        stream_batches,
    )
    from dlg.pipelines.stream import (
        replay_last_batch as replay,
    )
    from dlg.spark import get_spark

    if once and continuous:
        typer.echo("choose one of --once / --continuous", err=True)
        raise typer.Exit(code=2)

    spark = get_spark("dlg-stream")

    if replay_last_batch:
        result = replay(spark)
        typer.echo(result.summary_line())
        typer.echo(
            "replay guard held: zero rows written"
            if result.skipped_replay and not result.silver_inserted
            else "WARNING: the replay wrote rows"
        )
        _write_json(json_out, result.as_dict())
        return

    mode = "continuous" if continuous else "once"
    typer.echo(f"running streaming ingest in '{mode}' mode")
    results = run_stream(spark, mode=mode, processing_time=processing_time)
    if results:
        print_report(results)
        total_in = sum(r.silver_inserted for r in results)
        total_q = sum(r.quarantined for r in results)
        typer.echo(
            f"\n{len(results)} micro-batch(es): {total_in:,} rows merged into silver.sessions_stream, "
            f"{total_q:,} quarantined"
        )
    else:
        typer.echo("no new files to process")
    typer.echo("\nledger:")
    for row in stream_batches(spark):
        typer.echo(
            f"  {row['batch_id']} files={row['source_files']} read={row['rows_read']} "
            f"bronze+={row['bronze_inserted']} silver+={row['silver_inserted']} "
            f"quarantined={row['quarantined']} skipped={row['skipped_replay']}"
        )
    _write_json(json_out, [r.as_dict() for r in results])


if __name__ == "__main__":  # pragma: no cover
    main()
