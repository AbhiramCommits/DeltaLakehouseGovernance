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


def main() -> None:
    app()


if __name__ == "__main__":  # pragma: no cover
    main()
