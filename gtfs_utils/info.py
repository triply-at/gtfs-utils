from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from .bounds import get_bounding_box
from .cli_utils import SourceArgument, LazyOption
from .utils import load_gtfs, GtfsDict, compute_if_necessary

app = typer.Typer()


@app.command(help="Get information about a GTFS feed")
def info(
    src: SourceArgument,
    lazy: LazyOption = False,
):
    df_dict = load_gtfs(src, lazy=lazy)
    gtfs_info = get_info(df_dict)
    min_date, max_date = gtfs_info.calendar_date_range

    console = Console()

    console.print()
    console.print(f"Info on GTFS file `{src}`", style="bold underline")
    console.print()

    console.print("Bounding Box:\t", style="bold", end="")
    console.print(str(gtfs_info.bounding_box))

    console.print("Calendar date range:\t", style="bold", end="")
    console.print(f"{min_date.strftime('%d.%m.%Y')} - {max_date.strftime('%d.%m.%Y')}")
    console.print()

    table = Table(title="File Sizes")
    table.add_column("File", justify="center")
    table.add_column("Rows", justify="right")
    for file, size in gtfs_info.file_size.items():
        table.add_row(file, f"{size:_} rows")
    console.print(table)


@dataclass
class GtfsInfo:
    # Bounding box of stops in the gtfs feed. Tuple of (min_lon, min_lat, max_lon, max_lat)
    bounding_box: tuple[float, float, float, float]
    file_size: dict[str, int]
    calendar_date_range: tuple[datetime, datetime]


def get_info(src: Path | GtfsDict) -> GtfsInfo:
    """
    Get information about a GTFS feed.
    :param src: Path to GTFS directory or zip file, or a dictionary of DataFrames
    :return: a GtfsInfo object for the feed
    """
    df_dict = load_gtfs(src) if isinstance(src, Path) else src
    date_range = get_calendar_date_range(src)
    file_size = {}
    for file in df_dict:
        file_size[file] = len(df_dict[file])
    bounds = get_bounding_box(src)

    return GtfsInfo(bounds, file_size, date_range)


def get_calendar_date_range(df_dict: GtfsDict) -> tuple[datetime, datetime]:
    if "calendar" in df_dict:
        calendar = df_dict["calendar"]

        min_date = min(
            compute_if_necessary(
                calendar["start_date"].min(),
                calendar["end_date"].min(),
            )
        )
        max_date = max(
            compute_if_necessary(
                calendar["start_date"].max(),
                calendar["end_date"].max(),
            )
        )
    else:
        raise ValueError("calendar.txt missing")

    return (
        datetime.strptime(str(min_date), "%Y%m%d"),
        datetime.strptime(str(max_date), "%Y%m%d"),
    )
