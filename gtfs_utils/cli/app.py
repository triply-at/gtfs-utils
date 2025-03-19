import logging
from typing import Annotated, Optional

import typer

from gtfs_utils import __version__
from gtfs_utils.cli import filter, bounds, info

app = typer.Typer()
app.add_typer(info.app)
app.add_typer(bounds.app)
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
