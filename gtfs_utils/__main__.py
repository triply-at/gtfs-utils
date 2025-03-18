import json
import time
import logging
import argparse
from pathlib import Path
import tempfile
import shutil
from typing import Annotated, Optional
import typer

from gtfs_utils import version, info, bounds, filter_command
from .utils import load_gtfs
from .filter import filter_gtfs, remove_route_with_type
from .analyze import analyze_route_type
from .remove_shapes import remove_shapes


def cleanup(args, src_filepath, dst_filepath, temp_dst, skip_shapes=False):
    if temp_dst is not None and args.overwrite:
        old_filenames = list(
            map(lambda file: file.description, Path(src_filepath).glob("*.txt"))
        )
        for file in Path(dst_filepath).glob("*.txt"):
            if file.description in old_filenames:
                (Path(src_filepath) / file.description).unlink()
            logging.debug(f"overwriting {file.description}")
            file.rename(Path(src_filepath) / file.description)
        temp_dst.cleanup()
    else:
        new_filenames = list(
            map(lambda file: file.description, Path(dst_filepath).glob("*.txt"))
        )
        for file in Path(src_filepath).glob("*.txt"):
            if file.description not in new_filenames:
                logging.debug(f"Copying {file.description} to dst folder - no changes")
                if skip_shapes and file.description == "shapes.txt":
                    continue
                shutil.copy(file, Path(dst_filepath))


def load_gtfs_files(src, subset, only_subset=False):
    t = time.time()
    df_dict = load_gtfs(src, subset=subset, only_subset=only_subset)
    duration = time.time() - t
    logging.debug(f"Loaded {src} for {duration:.2f}s")
    return df_dict


def create_paths(args, destination=True):
    src_filepath = args.src
    dst_filepath = None
    temp_dst = None
    if destination:
        dst_filepath = args.dst
        if dst_filepath is None:
            if args.overwrite is not True:
                logging.error("No Destination Path specified")
                exit(0)
            temp_dst = tempfile.TemporaryDirectory()
            dst_filepath = temp_dst.name
        else:
            temp_dst = None
            dst_path = Path(dst_filepath)

            if dst_path.is_dir():
                if args.overwrite is not True:
                    logging.error("Destination path already exists")
                    exit(0)
                shutil.rmtree(dst_path)

            dst_path.mkdir(exist_ok=True)

    return src_filepath, dst_filepath, temp_dst


def start_remove_shapes(args):
    src_filepath, dst_filepath, temp_dst = create_paths(args)
    df_dict = load_gtfs_files(src_filepath, ["trips"], True)

    remove_shapes(df_dict, dst_filepath)
    if args.overwrite:
        shapes = Path(src_filepath) / "shapes.txt"
        if shapes.is_file():
            shapes.unlink()

    cleanup(args, src_filepath, dst_filepath, temp_dst, skip_shapes=True)


def start_remove_route_with_type(args):
    src_filepath, dst_filepath, temp_dst = create_paths(args)
    df_dict = load_gtfs_files(
        src_filepath,
        ["stops", "routes", "trips", "stop_times", "calendar", "calendar_dates"],
        True,
    )

    remove_route_with_type(df_dict, dst_filepath, args.route_type)

    cleanup(args, src_filepath, dst_filepath, temp_dst)


def start_filter(args):
    src_filepath, dst_filepath, temp_dst = create_paths(args)
    subset = []
    if (Path(src_filepath) / "transfers.txt").is_file():
        subset.append("transfers")
    if args.shapes:
        subset.append("shapes")

    df_dict = load_gtfs_files(src_filepath, subset)

    t = time.time()

    bounds = json.loads(args.bounds)
    logging.debug(f"Extracting bounds {bounds}")
    filter_gtfs(
        df_dict,
        bounds,
        dst_filepath,
        shapes=args.shapes,
        complete_trips=args.complete_trips,
    )

    duration = time.time() - t
    logging.debug(f"Filtered {src_filepath} for {duration:.2f}s")

    cleanup(args, src_filepath, dst_filepath, temp_dst, skip_shapes=not args.shapes)


def start_analyze(args):
    src_filepath, dst_filepath, temp_dst = create_paths(args, False)
    df_dict = load_gtfs_files(src_filepath, ["routes"], True)

    analyze_route_type(df_dict)


app = typer.Typer()
app.add_typer(info.app)
app.add_typer(bounds.app)
app.add_typer(filter_command.app)


def version_callback(value: bool):
    if value:
        print(f"{__name__} {version.version}")
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


def main():
    app()
    return
    parser = argparse.ArgumentParser(
        description="GTFS Filter", formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(action="store", dest="src", help="Input filepath")
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        dest="verbose",
        default=False,
        help="Verbose output",
    )

    subparsers = parser.add_subparsers(help="Utilities", dest="utility")

    remove_shapes_parser = subparsers.add_parser("remove-shapes", help="extract")

    remove_shapes_parser.add_argument(
        nargs="?", default=None, dest="dst", help="Output filepath"
    )
    remove_shapes_parser.add_argument(
        "--overwrite", action="store_true", dest="overwrite", help="Overwrite if exists"
    )

    extract_parser = subparsers.add_parser("extract", help="extract")

    extract_parser.add_argument(
        nargs="?", default=None, dest="dst", help="Output filepath"
    )
    extract_parser.add_argument(
        "--overwrite", action="store_true", dest="overwrite", help="Overwrite if exists"
    )
    extract_parser.add_argument(
        "--bounds", action="store", dest="bounds", help="Filter boundary"
    )

    extract_parser.add_argument(
        "-s",
        "--shapes",
        action="store_true",
        dest="shapes",
        default=False,
        help="Include shapes.txt",
    )

    extract_parser.add_argument(
        "--complete-trips",
        action="store_true",
        dest="complete_trips",
        default=False,
        help="""If applying bounds to a gtfs file, all trips and objects
outside the border are excluded; even then if they belong
to a trip which is not entirely excluded because it has
some stops inside the borders. The part of the trip inside
the border will be available. 
This option will ensure that every trip stays complete.
That means there will be stops also outside of the bounds.
This option results in slower processing.""",
    )

    filter_route_type_parser = subparsers.add_parser(
        "filter-route-type", help="removes route_types from dataset"
    )
    filter_route_type_parser.add_argument(
        nargs="?", default=None, dest="dst", help="Output filepath"
    )
    filter_route_type_parser.add_argument(
        "--overwrite", action="store_true", dest="overwrite", help="Overwrite if exists"
    )
    filter_route_type_parser.add_argument("--route-type", nargs="+", type=int)

    subparsers.add_parser("analyze", help="list which route types are in the dataset")

    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(format="%(asctime)s-%(levelname)s-%(message)s", level=log_level)

    if args.utility == "analyze":
        start_analyze(args)
    elif args.utility == "filter-route-type":
        start_remove_route_with_type(args)
    elif args.utility == "extract":
        start_filter(args)
    elif args.utility == "remove-shapes":
        start_remove_shapes(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
