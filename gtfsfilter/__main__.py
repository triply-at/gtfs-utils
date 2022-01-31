import json
import time
import logging
import argparse
from . import load_gtfs, save_gtfs
from .filter import filter_gtfs


def main():
    parser = argparse.ArgumentParser(
        description="GTFS Utilities")
    parser.add_argument(action='store',
        dest='src', help="Input filepath")
    parser.add_argument(action='store',
        dest='dst', help="Output filepath")
    parser.add_argument("--bounds", action='store',
        dest='bounds', help="Filter boundary")
    parser.add_argument("-o", "--operation", action='store',
        dest='operation', help="Filter operation (within, intersects)")
    parser.add_argument("--overwrite", action='store_true',
        dest='overwrite', help="Overwrite if exists")
    parser.add_argument('-v', '--verbose', action='store_true',
        dest='verbose', default=False,
        help="Verbose output")
    parser.add_argument('-s', '--shapes', action='store_true',
        dest='shapes', default=False,
        help="Include shapes.txt")
    parser.add_argument('-t', '--transfers', action='store_true',
        dest='transfers', default=False,
        help="Include transfers.txt")
    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        format='%(asctime)s-%(levelname)s-%(message)s',
        level=log_level)

    bounds = json.loads(args.bounds)
    src_filepath = args.src
    dst_filepath = args.dst

    # Load GTFS
    t = time.time()
    df_dict = load_gtfs(src_filepath, subset=[args.shapes, args.transfers])
    duration = time.time() - t
    logging.debug(f"Loaded {src_filepath} for {duration:.2f}s")

    # Filter GTFS
    t = time.time()
    filter_gtfs(df_dict, bounds, dst_filepath)
    duration = time.time() - t
    logging.debug(f"Filtered {src_filepath} for {duration:.2f}s")


if __name__ == '__main__':
    main()
