import logging
from typing import Annotated, Optional

import typer
from rich.console import Console

from gtfs_utils import (
    __version__,
    get_bounding_box,
    load_gtfs_delayed,
)
from gtfs_utils.cli import filter, info
from gtfs_utils.cli.cli_utils import SourceArgument, LazyOption
from gtfs_utils.info import get_route_type_counts

app = typer.Typer()
app.add_typer(info.app)
app.add_typer(filter.app)


def version_callback(value: bool):
    if value:
        print(f"{__name__} {__version__}")
        raise typer.Exit()


# noinspection PyUnusedLocal
@app.callback()
def common(
    print_version: Annotated[
        Optional[bool],
        typer.Option(
            "--version/",
            help="Print version and exit",
            callback=version_callback,
            is_eager=True,
        ),
    ] = False,
    verbose: Annotated[
        Optional[bool],
        typer.Option(
            "--verbose",
            "-v",
            help="Verbose output",
        ),
    ] = False,
):
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        format="[%(asctime)s] %(levelname)s %(message)s", level=log_level
    )


@app.command(help="Get the bounding box of a GTFS feed")
def bounds(
    src: SourceArgument,
    lazy: LazyOption = False,
):
    df_dict = load_gtfs_delayed(src, lazy=lazy)
    bbox = get_bounding_box(df_dict)

    console = Console()
    console.print("Bounding Box:\t", style="bold", end="")
    console.print(str(list(bbox)))


@app.command(help="List existing route types and number of routes in a GTFS feed")
def route_types(
    src: SourceArgument,
    lazy: LazyOption = False,
    _list: Annotated[
        Optional[bool],
        typer.Option(
            "--list/", help="Show a list of included route types instead of counts"
        ),
    ] = False,
):
    df_dict = load_gtfs_delayed(src, lazy=lazy)
    route_counts = get_route_type_counts(df_dict)

    console = Console()
    console.print("Route Types:\t", style="bold", end="")
    if list:
        console.print(str(sorted(route_counts.keys())))
    else:
        console.print(str(route_counts))
