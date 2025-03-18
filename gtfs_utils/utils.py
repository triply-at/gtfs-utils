import logging
from dataclasses import dataclass
from enum import Enum
from typing import Dict, TypeAlias
from zipfile import ZipFile

import dask.dataframe as dd
from pathlib import Path
import pandas as pd
from dask import is_dask_collection


@dataclass
class GtfsFileMixin:
    file: str
    required: bool = True


class GtfsFile(GtfsFileMixin, Enum):
    AGENCY = "agency"
    STOPS = "stops"
    ROUTES = "routes"
    TRIPS = "trips"
    CALENDAR = "calendar"
    CALENDAR_DATES = "calendar_dates"
    STOP_TIMES = "stop_times"
    SHAPES = "shapes", False
    FREQUENCIES = "frequencies", False
    FEED_INFO = "feed_info", False


GtfsDict: TypeAlias = Dict[str, dd.DataFrame | pd.DataFrame]

REQUIRED_FILES = [f for f in GtfsFile if f.required]

# https://developers.google.com/transit/gtfs/reference
DTYPES = {
    "agency_id": "string",
    "agency_name": "string",
    "agency_url": "string",
    "agency_timezone": "string",
    "agency_lang": "string",
    "agency_phone": "string",
    "stop_headsign": "string",
    "shape_id": "string",
    "shape_pt_sequence": "uint64",
    "shape_pt_lat": "float64",
    "shape_pt_lon": "float64",
    "shape_dist_traveled": "float64",
    "stop_lat": "float64",
    "location_type": "UInt8",
    "monday": "uint8",
    "tuesday": "uint8",
    "wednesday": "uint8",
    "thursday": "uint8",
    "friday": "uint8",
    "saturday": "uint8",
    "sunday": "uint8",
    "direction_id": "UInt8",
    "route_type": "uint16",
    "transfer_type": "uint8",
    "pickup_type": "UInt8",
    "drop_off_type": "UInt8",
    "min_transfer_time": "UInt64",
    "exception_type": "uint8",
    "parent_station": "string",
    "stop_sequence": "uint64",
    "stop_lon": "float64",
    "route_id": "string",
    "service_id": "string",
    "date": "UInt64",
    "start_date": "uint64",
    "end_date": "uint64",
    "trip_id": "string",
    "trip_headsign": "string",
    "arrival_time": "string",
    "departure_time": "string",
    "stop_id": "string",
    "from_stop_id": "string",
    "to_stop_id": "string",
    "zone_id": "string",
    "block_id": "string",
    "wheelchair_accessible": "UInt8",
    "bikes_allowed": "UInt8",
    "stop_code": "string",
    "stop_name": "string",
    "route_long_name": "string",
    "route_short_name": "string",
    "platform_code": "string",
}


def load_gtfs(
    filepath: str | Path,
    subset: None | list[str] = None,
    lazy=True,
    only_subset=False,
) -> GtfsDict:
    """
    Load GTFS data from a directory or zip file.

    :param filepath: Path to GTFS directory or zip file
    :param subset: List of GTFS files to load in addition to the required files
    :param lazy: If `True`, return dask Dataframes, otherwise returns pandas Dataframes
    :param only_subset: Only load files in subset
    :return: a dict of gtfs file names to Dataframes.
    """
    if subset is None:
        subset = []

    df_dict: Dict[str, pd.DataFrame | dd.DataFrame] = {}
    p = Path(filepath)

    files_to_read = (
        subset if only_subset else subset + [file.file for file in REQUIRED_FILES]
    )

    if p.is_dir():
        for file in p.iterdir():
            if file.is_file():
                file_key = file.stem
                if file_key in files_to_read:
                    logging.debug(f"Reading {file_key}")
                    sample_df = pd.read_csv(file, nrows=2)

                    for col in sample_df.columns:
                        if col not in DTYPES:
                            logging.warning(col + " not in dtypes - using type string")
                            DTYPES[col] = "string"

                    df_dict[file_key] = (dd if lazy else pd).read_csv(
                        file,
                        low_memory=False,
                        dtype=DTYPES,
                    )

    elif p.suffix == ".zip":
        with ZipFile(filepath) as zip_file:
            for file in zip_file.namelist():
                file_key = Path(file).stem
                if file_key in files_to_read:
                    logging.debug(f"Reading {file_key}")
                    sample_df = pd.read_csv(zip_file.open(file), nrows=2)

                    for col in sample_df.columns:
                        if col not in DTYPES:
                            logging.warning(col + " not in dtypes - using type string")
                            DTYPES[col] = "string"

                    df_dict[file_key] = (dd if lazy else pd).read_csv(
                        zip_file.open(file), low_memory=False, dtype=DTYPES
                    )
    else:
        raise Exception(f"{p} is no directory or zipfile")

    return df_dict

def compute_if_necessary(*args):
    """
    Computes the incoming args if necessary
    :param args:
    :return: args, if args are not a dask collection, otherwise the computed args
    """
    if args is None:
        return args
    if is_dask_collection(args[0]):
        return dd.compute(*args)

    return args