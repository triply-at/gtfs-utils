from pathlib import Path
from typing import Annotated

import typer

from gtfs_utils import load_gtfs_delayed
from gtfs_utils.cli.cli_utils import LazyOption, SourceArgument
from gtfs_utils.filter import BoundsFilter, RouteTypeFilter, filter_gtfs
from gtfs_utils.info import _has_active_service
from gtfs_utils.utils import Timer

app = typer.Typer()


class Bounds:
    def __init__(self, bounds: list[float]):
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
        Bounds | None,
        typer.Option(
            "--bounds",
            "-b",
            help="Bounding box to filter by. In the format of `[minLon, minLat, maxLon, maxLat]`",
            parser=parse_bounds,
        ),
    ] = None,
    complete_trips: Annotated[
        bool | None,
        typer.Option(
            "--complete-trips",
            help="Keep trips complete, even if some stops are outside bounds",
        ),
    ] = True,
    filter_route_types: Annotated[
        str | None,
        typer.Option(
            "--filter-route-types",
            help="Route types to filter by, e.g. `0,1,2`. All routes with a different type will be removed. ",
        ),
    ] = None,
    exclude_route_types: Annotated[
        str | None,
        typer.Option(
            "--remove-route-types", help="Route types to remove, e.g. `0,1,2`"
        ),
    ] = None,
    lazy: LazyOption = False,
    overwrite: Annotated[
        bool | None,
        typer.Option(
            "--overwrite",
            "-f",
            help="Overwrite output directory if it exists",
        ),
    ] = False,
    skip_empty: Annotated[
        bool,
        typer.Option(
            "--skip-empty",
            help="Write no output when the filtered feed has no trips or service days",
        ),
    ] = False,
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

    df_dict = load_gtfs_delayed(src, lazy=lazy)

    with Timer("Finished filtering in %.2f seconds"):
        filtered = filter_gtfs(df_dict, filters)

    if skip_empty and not _has_active_service(filtered):
        if overwrite:
            typer.echo(
                f'Error: Refusing to overwrite "{output}" with an empty GTFS feed.',
                err=True,
            )
            raise typer.Exit(code=1)

        typer.echo(
            f'Skipped "{output}": filtered GTFS feed is empty; no output written.'
        )
        return

    filtered.save(output_dir_or_file=output, overwrite=overwrite)
    print(f'Wrote output to "{output}"')
