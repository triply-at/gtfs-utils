from collections import Counter
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.markup import escape
from rich.table import Table

from gtfs_utils import load_gtfs_delayed
from gtfs_utils.cli.cli_utils import SourceArgument
from gtfs_utils.patch import (
    Patch,
    PatchError,
    PatchLoadError,
    PatchReport,
    apply_patch,
    load_patch,
    yaml_available,
)
from gtfs_utils.patch.loader import YAML_INSTALL_HINT, YAML_SUFFIXES
from gtfs_utils.utils import Timer

app = typer.Typer(help="Apply patches to a GTFS feed")

EXIT_ABORTED = 1
EXIT_LOAD_FAILED = 2
SKIPPED_TRIPS_SHOWN = 5


@app.command(name="apply", help="Apply one or more patch files to a GTFS feed")
def apply_app(
    src: SourceArgument,
    patches: Annotated[
        list[Path],
        typer.Argument(
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            help="Patch files (.yaml, .yml or .json), applied in the given order",
        ),
    ],
    output: Annotated[
        Path | None,
        typer.Option(
            "--output",
            "-o",
            help="Output GTFS filepath",
            file_okay=True,
            dir_okay=True,
            writable=True,
        ),
    ] = None,
    overwrite: Annotated[
        bool,
        typer.Option("--overwrite", "-f", help="Overwrite output if it exists"),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Apply and print the report, write nothing"),
    ] = False,
):
    console = Console()

    if output is None and not dry_run:
        _fail(console, "--output is required unless --dry-run is given")
    if output is not None and output.exists() and not overwrite and not dry_run:
        _fail(console, f'"{output}" already exists, use -f to overwrite')
    if any(p.suffix.lower() in YAML_SUFFIXES for p in patches) and not yaml_available():
        _fail(console, YAML_INSTALL_HINT)

    try:
        patch = _combine([load_patch(p) for p in patches])
    except PatchLoadError as e:
        _fail(console, str(e))

    gtfs = load_gtfs_delayed(src, lazy=False)
    try:
        with Timer("Applied patch in %.2f seconds"):
            patched, report = apply_patch(gtfs, patch)
    except PatchError as e:
        console.print(f"[bold red]Aborted, nothing written:[/] {escape(str(e))}")
        raise typer.Exit(code=EXIT_ABORTED)

    print_report(console, report)
    if patch.base_feed:
        console.print("[dim]base_feed is not checked yet[/]")
    if dry_run:
        console.print("Dry run, no output written")
        return

    patched.save(output_dir_or_file=output, overwrite=overwrite)
    console.print(f'Wrote output to "{escape(str(output))}"')


def _combine(patches: list[Patch]) -> Patch:
    return Patch(
        version=1,
        ops=tuple(op for p in patches for op in p.ops),
        base_feed=patches[0].base_feed,
    )


def _fail(console: Console, message: str) -> None:
    console.print(f"[bold red]Error:[/] {escape(message)}")
    raise typer.Exit(code=EXIT_LOAD_FAILED)


def print_report(console: Console, report: PatchReport) -> None:
    table = Table(title="Patch report")
    table.add_column("Op")
    table.add_column("Selected", justify="right")
    table.add_column("Patched per direction")
    table.add_column("Skipped", justify="right")
    table.add_column("Stops added")
    table.add_column("Demands added", justify="right")
    for op in report.ops:
        table.add_row(
            escape(op.op_id),
            str(op.trips_selected),
            ", ".join(f"{d}: {n}" for d, n in op.trips_patched.items()),
            str(len(op.skipped)),
            escape("\n".join(op.stops_added)) or "-",
            str(op.demands_added),
        )
    console.print(table)

    for op in report.ops:
        for code, count in Counter(s.code for s in op.skipped).items():
            trip_ids = [s.trip_id for s in op.skipped if s.code == code]
            shown = escape(", ".join(trip_ids[:SKIPPED_TRIPS_SHOWN]))
            more = (
                f" and {count - SKIPPED_TRIPS_SHOWN} more"
                if count > SKIPPED_TRIPS_SHOWN
                else ""
            )
            console.print(
                f"{escape(op.op_id)}: skipped {count} trip(s) [bold]{code}[/]: {shown}{more}"
            )
