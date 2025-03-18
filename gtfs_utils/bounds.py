from typing import Annotated

import typer
from rich.console import Console

from . import GtfsFile
from .cli_utils import SourceArgument, LazyOption
from .utils import load_gtfs, GtfsDict, compute_if_necessary

app = typer.Typer()


@app.command(help="Get the bounding box of a GTFS feed")
def bounds(
    src: SourceArgument,
    lazy: LazyOption = False,
):
    df_dict = load_gtfs(src, lazy=lazy, subset=[GtfsFile.STOPS.file], only_subset=True)
    bbox = get_bounding_box(df_dict)

    console = Console()
    console.print("Bounding Box:\t", style="bold", end="")
    console.print(str(bbox))


def get_bounding_box(df_dict: GtfsDict) -> tuple[float, float, float, float]:
    stops = df_dict["stops"]
    return compute_if_necessary(
        stops["stop_lon"].min(),
        stops["stop_lat"].min(),
        stops["stop_lon"].max(),
        stops["stop_lat"].max(),
    )
