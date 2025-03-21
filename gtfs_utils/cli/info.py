import typer
from rich.console import Console
from rich.table import Table

from gtfs_utils import load_gtfs, get_info
from gtfs_utils.cli.cli_utils import SourceArgument, LazyOption
from gtfs_utils.utils import ROUTE_TYPES

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
    console.print(str(list(gtfs_info.bounding_box)))

    console.print("Calendar date range:\t", style="bold", end="")
    console.print(f"{min_date.strftime('%d.%m.%Y')} - {max_date.strftime('%d.%m.%Y')}")

    console.print()
    table = Table(title="File Sizes", style="bold")
    table.add_column("File", justify="left")
    table.add_column("Rows", justify="right")
    for file, size in gtfs_info.file_size.items():
        table.add_row(file, f"{size:_} rows")
    console.print(table)

    console.print()
    table = Table(title="Route Types", style="bold")
    table.add_column("Route Type", justify="center")
    table.add_column("Route Type ID", justify="center")
    table.add_column("# Routes", justify="right")
    for type_int, count in gtfs_info.route_type_counts.items():
        route_type_str = (
            ROUTE_TYPES[type_int] if type_int in ROUTE_TYPES else "Unknown Type"
        )
        table.add_row(route_type_str, str(type_int), f"{count:_} routes")
    console.print(table)
