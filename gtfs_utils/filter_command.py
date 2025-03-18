import logging
import shutil
import tempfile
from pathlib import Path
from typing import Annotated, Optional, List

import typer

from gtfs_utils import load_gtfs
from gtfs_utils.cli_utils import SourceArgument, LazyOption
from gtfs_utils.filter import do_filter, BoundsFilter
from gtfs_utils.utils import OPTIONAL_FILES, Timer

app = typer.Typer()


class Bounds:
    def __init__(self, bounds: List[float]):
        self.bounds = bounds


def parse_bounds(bounds: str) -> Bounds:
    if not (bounds.startswith("[") and bounds.endswith("]")):
        raise ValueError(
            "Bounds must be in the format of `[minLat, minLon, maxLat, maxLon]`"
        )
    bounds = list(map(float, bounds.strip("[]").split(",")))
    if len(bounds) != 4:
        raise ValueError(
            "Bounds must be in the format of `[minLat, minLon, maxLat, maxLon]`"
        )

    if bounds[0] > bounds[2] or bounds[1] > bounds[3]:
        raise ValueError("Invalid bounds given")

    return Bounds(bounds)


@app.command(help="Filter a GTFS feed", name="filter")
def filter_function(
    src: SourceArgument,
    output: Annotated[
        Path,
        typer.Option(
            "--output",
            "-o",
            help="Output GTFS filepath",
            file_okay=True,
            dir_okay=True,
            writable=True,
        ),
    ],
    bounds: Annotated[
        Optional[Bounds],
        typer.Option(
            "--bounds",
            "-b",
            help="Bounding box to filter by. In the format of `[minLat, minLon, maxLat, maxLon]`",
            parser=parse_bounds,
        ),
    ] = None,
    complete_trips: Annotated[
        Optional[bool],
        typer.Option(
            "--complete-trips",
            help="Keep trips complete, even if some stops are outside bounds",
        ),
    ] = True,
    lazy: LazyOption = False,
):
    df_dict = load_gtfs(src, lazy=lazy, subset=OPTIONAL_FILES)
    temp_dir = Path(tempfile.mkdtemp())

    filters = []
    if bounds is not None:
        filters.append(
            BoundsFilter(
                bounds=bounds.bounds,
                complete_trips=complete_trips,
            )
        )

    with Timer("Finished filtering in %.2f seconds"):
        do_filter(df_dict, filters)
    # filter_gtfs(df_dict, bounds.bounds, temp_dir, True, complete_trips=complete_trips)
    logging.debug(f'Wrote file to temp directory "{temp_dir}"')

    # copy tempdir to output
    if output.exists() or output.suffix == ".zip":
        raise NotImplementedError(
            "Output file exists or is a zip file - currently not yet supported"
        )

    shutil.copytree(temp_dir, output)

    print(f'Wrote output to "{output}"')
