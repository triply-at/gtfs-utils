import typer
from rich.console import Console

from gtfs_utils import GtfsFile, get_bounding_box
from gtfs_utils.cli.cli_utils import SourceArgument, LazyOption
from gtfs_utils.utils import load_gtfs

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
    console.print(str(list(bbox)))
