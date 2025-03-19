import typer
from rich.console import Console
from rich.table import Table

from gtfs_utils import load_gtfs, get_info
from gtfs_utils.cli.cli_utils import SourceArgument, LazyOption


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
