import json
import time
import logging
import argparse
from pathlib import Path
import tempfile
import shutil

from . import load_gtfs
from .filter import filter_gtfs, remove_route_with_type
from .analyze import analyze_route_type
from .remove_shapes import remove_shapes


def main():
    parser = argparse.ArgumentParser(description="GTFS Filter", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument(action="store", dest="src", help="Input filepath")
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        dest="verbose",
        default=False,
        help="Verbose output",
    )

    subparsers = parser.add_subparsers(help='Utilities', dest='utility')

    remove_shapes_parser = subparsers.add_parser('remove-shapes', help='extract')

    remove_shapes_parser.add_argument("--overwrite", action="store_true", dest="overwrite", help="Overwrite if exists")
    remove_shapes_parser.add_argument(nargs='?', default=None, dest="dst", help="Output filepath")
    remove_shapes_parser.add_argument("--overwrite", action="store_true", dest="overwrite", help="Overwrite if exists")

    extract_parser = subparsers.add_parser('extract', help='extract')

    extract_parser.add_argument(nargs='?', default=None, dest="dst", help="Output filepath")
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
This option results in slower processing.""", )
    extract_parser.add_argument(
        "-t",
        "--transfers",
        action="store_true",
        dest="transfers",
        default=False,
        help="Include transfers.txt",
    )

    filter_route_type_parser = subparsers.add_parser('filter-route-type', help="removes route_types from dataset")
    filter_route_type_parser.add_argument(nargs='?', default=None, dest="dst", help="Output filepath")
    filter_route_type_parser.add_argument(
        "--overwrite", action="store_true", dest="overwrite", help="Overwrite if exists"
    )
    filter_route_type_parser.add_argument('--route-type', nargs='+', type=int)
    filter_route_type_parser.add_argument(
        "-s",
        "--shapes",
        action="store_true",
        dest="shapes",
        default=False,
        help="Include shapes.txt",
    )

    analyze = subparsers.add_parser('analyze', help="list which route types are in the dataset")

    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(format="%(asctime)s-%(levelname)s-%(message)s", level=log_level)

    src_filepath = args.src
    dst_filepath = args.dst
    if dst_filepath is None:
        if args.overwrite is not True:
            logging.error('No Destination Path specified')
            return
        temp_dst = tempfile.TemporaryDirectory()
        dst_filepath = temp_dst.name
    else:
        temp_dst = None
        Path(dst_filepath).mkdir(exist_ok=True)

    subset = []
    if 'analyze' != args.utility:
        if 'filter-route-type' != args.utility:
            if args.transfers:
                subset.append('transfers')
        if args.shapes:
            subset.append('shapes')

    # Load GTFS
    t = time.time()
    df_dict = load_gtfs(src_filepath, subset=subset)
    duration = time.time() - t
    logging.debug(f"Loaded {src_filepath} for {duration:.2f}s")

    if 'analyze' == args.utility:
        analyze_route_type(df_dict)
        return

    # Filter GTFS
    t = time.time()
    if 'filter-route-type' == args.utility:
        logging.debug(f"Removing route-types {args.route_type}")
        remove_route_with_type(df_dict, dst_filepath, args.route_type)
    elif 'extract' == args.utility:
        bounds = json.loads(args.bounds)
        logging.debug(f"Extracting bounds {bounds}")
        filter_gtfs(df_dict, bounds, dst_filepath, transfers=args.transfers, shapes=args.shapes,
                    complete_trips=args.complete_trips)
    elif 'remove-shapes' == args.utility:
        remove_shapes(df_dict, dst_filepath)
        if args.overwrite:
            shapes = Path(src_filepath) / 'shapes.txt'
            if shapes.is_file():
                shapes.unlink()
    else:
        parser.print_help()

    if temp_dst is not None and args.overwrite:
        old_filenames = list(map(lambda file: file.name, Path(src_filepath).glob('*.txt')))
        for file in Path(dst_filepath).glob('*.txt'):
            if file.name in old_filenames:
                (Path(src_filepath) / file.name).unlink()
            logging.debug(f'overwriting {file.name}')
            file.rename(Path(src_filepath) / file.name)
        temp_dst.cleanup()
    else:
        new_filenames = list(map(lambda file: file.name, Path(dst_filepath).glob('*.txt')))
        for file in Path(src_filepath).glob('*.txt'):
            if file.name not in new_filenames:
                logging.debug(f'Copying {file.name} to dst folder - no changes')
                if file.name == 'shapes.txt' and not args.shapes:
                    continue
                if file.name == 'transfers.txt' and not args.transfers:
                    continue
                shutil.copy(file, Path(dst_filepath))
    duration = time.time() - t
    logging.debug(f"Filtered {src_filepath} for {duration:.2f}s")


if __name__ == "__main__":
    main()
