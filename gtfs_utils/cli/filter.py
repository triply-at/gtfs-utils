from pathlib import Path
from typing import Annotated, Optional, List

import typer

from gtfs_utils import load_gtfs
from gtfs_utils.cli.cli_utils import SourceArgument, LazyOption
from gtfs_utils.filter import filter_gtfs, BoundsFilter, RouteTypeFilter
from gtfs_utils.utils import Timer, OPTIONAL_FILE_NAMES

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
def filter_app(
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
            help="Bounding box to filter by. In the format of `[minLon, minLat, maxLon, maxLat]`",
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
    filter_route_types: Annotated[
        Optional[str],
        typer.Option(
            "--filter-route-types",
            help="Route types to filter by, e.g. `0,1,2`. All routes with a different type will be removed. ",
        ),
    ] = None,
    exclude_route_types: Annotated[
        Optional[str],
        typer.Option(
            "--remove-route-types", help="Route types to remove, e.g. `0,1,2`"
        ),
    ] = None,
    lazy: LazyOption = False,
):
    if filter_route_types is not None and exclude_route_types is not None:
        raise ValueError(
            'Cannot use both "--filter-route-types" and "--remove-route-types"'
        )

    filters = []
    if bounds is not None:
        filters.append(
            BoundsFilter(
                bounds=bounds.bounds,
                complete_trips=complete_trips,
            )
        )
    if filter_route_types is not None:
        route_types = list(map(int, filter_route_types.split(",")))
        filters.append(
            RouteTypeFilter(
                route_types=route_types,
                negate=False,
            )
        )
    if exclude_route_types is not None:
        route_types = list(map(int, exclude_route_types.split(",")))
        filters.append(
            RouteTypeFilter(
                route_types=route_types,
                negate=True,
            )
        )

    df_dict = load_gtfs(src, lazy=lazy, subset=OPTIONAL_FILE_NAMES)

    with Timer("Finished filtering in %.2f seconds"):
        filtered = filter_gtfs(df_dict, filters)
    filtered.save(output_dir=output)
    print(f'Wrote output to "{output}"')
