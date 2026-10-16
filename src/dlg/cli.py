"""Typer CLI for the Delta lakehouse.

Run ``python -m dlg.cli --help`` for the full command list, or use the
``make`` targets which wrap the common flows.
"""

from __future__ import annotations

import typer

from dlg.config import get_settings

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Delta Lakehouse Governance — medallion lakehouse, governance, quality and agent CLI.",
)


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


def main() -> None:
    app()


if __name__ == "__main__":  # pragma: no cover
    main()
