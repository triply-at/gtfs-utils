from pathlib import Path
from typing import Annotated

import typer

SourceArgument = Annotated[
    Path,
    typer.Argument(
        exists=True,
        file_okay=True,
        dir_okay=True,
        readable=True,
        help="Path to GTFS directory or file",
    ),
]

LazyOption = Annotated[bool, typer.Option(help="Use dask to load files")]
