import logging
import shutil
import time
from collections.abc import MutableMapping
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import IO, Callable, List, Literal, TypeVar
from zipfile import ZIP_DEFLATED, ZipFile

import dask.dataframe as dd
import pandas as pd
from dask import is_dask_collection
from typing_extensions import deprecated


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
    TRANSFERS = "transfers", False


class GtfsDict(MutableMapping[str, pd.DataFrame | dd.DataFrame]):
    def __init__(self, *args, **kwargs) -> None:
        self.store = {}
        self.update(dict(*args, **kwargs))

    def __getitem__(self, key, /):
        return self.store[key]

    def __setitem__(self, key, value, /):
        self.store[key] = value

    def __delitem__(self, key, /):
        del self.store[key]

    def __iter__(self):
        return iter(self.store)

    def __len__(self):
        return len(self.store)

    def __contains__(self, item):
        return item in self.store

    @classmethod
    def load(cls, *args, **kwargs) -> "GtfsDict":
        return load_gtfs_delayed(*args, **kwargs)

    def save_file(
        self,
        file: str,
        output: Path | str | IO,
    ) -> None:
        """
        Save a file to the output directory
        :param file: the gtfs file to save
        :param output: path to gtfs folder to store data in or a buffer to write the data into.
        :return:
        """
        if file not in self:
            raise KeyError(f"{file} not found in GTFS data")
        df = self[file]

        if isinstance(file, str) and (
            isinstance(output, Path) or isinstance(output, str)
        ):
            output = Path(output) / f"{file}.txt"

        save_kwargs = (
            {"single_file": True, "index": False}
            if is_dask_collection(df)
            else {"index": False}
        )
        df.to_csv(output, **save_kwargs)

    def save(self, output_dir_or_file: Path | str, overwrite=False) -> None:
        output = Path(output_dir_or_file)

        if output.exists():
            if overwrite:
                logging.warning(f"{output} already exists but will be deleted.")
                if output.is_file():
                    output.unlink()
                else:
                    shutil.rmtree(output)
            else:
                raise FileExistsError(
                    f"{output} already exists. Use overwrite=True / `-f` to overwrite the file."
                )

        if output.suffix == ".zip":
            with ZipFile(output, "w", compression=ZIP_DEFLATED) as zip_file:
                for file in self:
                    with zip_file.open(f"{file}.txt", "w") as f:
                        self.save_file(file, f)

        else:
            if not output.exists():
                output.mkdir(parents=True)
            for file in self:
                self.save_file(file, output)

    def stop_gdf(self, additional_columns: List[str] | Literal["all"] | None = None):
        """
        Returns a GeoDataFrame of stops
        :param additional_columns: additional columns to include in the GeoDataFrame, per default only ID is included
        :return:
        """
        import geopandas as gpd

        df: pd.DataFrame = self.stops()

        if additional_columns is None:
            df = df[["stop_lon", "stop_lat", "stop_id"]]
        elif additional_columns == "all":
            # no further filtering - all columns
            pass
        elif isinstance(additional_columns, list):
            df = df[["stop_lon", "stop_lat"] + additional_columns]

        df = compute_if_necessary(df)
        return gpd.GeoDataFrame(
            df.drop(columns=["stop_lon", "stop_lat"], axis=0),
            geometry=gpd.points_from_xy(df.stop_lon, df.stop_lat),
            crs="EPSG:4326",
        )

    def agency(self) -> pd.DataFrame | dd.DataFrame:
        return self[GtfsFile.AGENCY.file]

    def stops(self) -> pd.DataFrame | dd.DataFrame:
        return self[GtfsFile.STOPS.file]

    def routes(self) -> pd.DataFrame | dd.DataFrame:
        return self[GtfsFile.ROUTES.file]

    def trips(self) -> pd.DataFrame | dd.DataFrame:
        return self[GtfsFile.TRIPS.file]

    def calendar(self) -> pd.DataFrame | dd.DataFrame:
        return self[GtfsFile.CALENDAR.file]

    def calendar_dates(self) -> pd.DataFrame | dd.DataFrame:
        return self[GtfsFile.CALENDAR_DATES.file]

    def stop_times(self) -> pd.DataFrame | dd.DataFrame:
        return self[GtfsFile.STOP_TIMES.file]

    def shapes(self) -> pd.DataFrame | dd.DataFrame:
        return self[GtfsFile.SHAPES.file]

    def frequencies(self) -> pd.DataFrame | dd.DataFrame:
        return self[GtfsFile.FREQUENCIES.file]

    def feed_info(self) -> pd.DataFrame | dd.DataFrame:
        return self[GtfsFile.FEED_INFO.file]

    def transfers(self) -> pd.DataFrame | dd.DataFrame:
        return self[GtfsFile.TRANSFERS.file]

    def filter(
        self,
        file: str | GtfsFile,
        where: Callable[[pd.DataFrame], pd.Series],
        return_cols: str | List[str] = None,
    ) -> pd.Series | pd.DataFrame | None:
        """
        Filter a file in the GTFS feed by a where condition.
        All data not matching the condition will be removed from the file.

        If the file does not exist, an empty DataFrame is returned.

        :param file: the feed to filter
        :param where: condition to filter by - should return a boolean series of the same size as the incoming Dataframe
        :param return_cols: columns of the filtered data to return. If no columns are given, `None` is returned
        :return: the filtered data or None
        """
        if not self.__contains__(file):
            if return_cols is None:
                return None
            if isinstance(return_cols, str):
                return pd.Series()
            else:
                return pd.DataFrame(columns=return_cols)

        mask = where(self[file])
        self[file] = self[file][mask]  # noqa
        if return_cols is None:
            return None

        if isinstance(return_cols, str):
            if return_cols not in self[file].columns:
                return pd.Series()

            return compute_if_necessary(self[file][return_cols])

        existing_cols = list(set(return_cols) & set(self[file].columns))

        return compute_if_necessary(self[file][existing_cols])

    def bounds(self) -> tuple[float, float, float, float]:
        from gtfs_utils.info import get_bounding_box

        return get_bounding_box(self)


class DelayedGtfsDict(GtfsDict):
    def __init__(
        self, base_file: Path, existing_files: dict[str, str], lazy: bool = False
    ) -> None:
        super().__init__()
        self.base_file = base_file
        self.existing_files = existing_files
        self.lazy = lazy

    def __iter__(self):
        for key in self.existing_files:
            _ = self[key]
            yield key

    def __getitem__(self, item):
        if super().__contains__(item):
            return super().__getitem__(item)

        if item not in self.existing_files.keys():
            raise KeyError(f"{item} not found in GTFS data")

        file = self.read_file(item)
        super().__setitem__(item, file)
        return file

    def __contains__(self, item):
        return super().__contains__(item) or item in self.existing_files.keys()

    def read_file(self, item: str) -> pd.DataFrame | dd.DataFrame:
        if self.base_file.is_dir():
            file_path = self.existing_files[item]
            return _read_from_folder(file_path, self.lazy)

        else:
            with ZipFile(self.base_file) as zip_file:
                return _read_from_zipped(
                    self.existing_files[item], self.lazy, self.base_file, zip_file
                )


REQUIRED_FILES: List[GtfsFile] = [f for f in GtfsFile if f.required]
OPTIONAL_FILES: List[GtfsFile] = [f for f in GtfsFile if not f.required]
OPTIONAL_FILE_NAMES: List[str] = [f.file for f in OPTIONAL_FILES]

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

ROUTE_TYPES = {
    0: "Tram, Streetcar, Light rail",
    1: "Subway, Metro",
    2: "Rail",
    3: "Bus",
    4: "Ferry",
    5: "Cable car",
    6: "Aerial lift, suspended cable car",
    7: "Funicular",
    11: "Trolleybus",
    12: "Monorail",
    # extended route types -
    # see https://developers.google.com/transit/gtfs/reference/extended-route-types for more details
    # 00: 'Railway Service',
    101: "High Speed Rail Service",
    102: "Long Distance Trains",
    103: "Inter Regional Rail Service",
    104: "Car Transport Rail Service",
    105: "Sleeper Rail Service",
    106: "Regional Rail Service",
    107: "Tourist Railway Service",
    108: "Rail Shuttle (Within Complex)",
    109: "Suburban Railway",
    110: "Replacement Rail Service",
    111: "Special Rail Service",
    112: "Lorry Transport Rail Service",
    113: "All Rail Services",
    114: "Cross-Country Rail Service",
    115: "Vehicle Transport Rail Service",
    116: "Rack and Pinion Railway",
    117: "Additional Rail Service",
    200: "Coach Service",
    201: "International Coach Service",
    202: "National Coach Service",
    203: "Shuttle Coach Service",
    204: "Regional Coach Service",
    205: "Special Coach Service",
    206: "Sightseeing Coach Service",
    207: "Tourist Coach Service",
    208: "Commuter Coach Service",
    209: "All Coach Services",
    400: "Urban Railway Service",
    401: "Metro Service",
    402: "Underground Service",
    403: "Urban Railway Service",
    404: "All Urban Railway Services",
    405: "Monorail",
    700: "Bus Service",
    701: "Regional Bus Service",
    702: "Express Bus Service",
    703: "Stopping Bus Service",
    704: "Local Bus Service",
    705: "Night Bus Service",
    706: "Post Bus Service",
    707: "Special Needs Bus",
    708: "Mobility Bus Service",
    709: "Mobility Bus for Registered Disabled",
    710: "Sightseeing Bus",
    711: "Shuttle Bus",
    712: "School Bus",
    713: "School and Public Service Bus",
    714: "Rail Replacement Bus Service",
    715: "Demand and Response Bus Service",
    716: "All Bus Services",
    800: "Trolleybus Service",
    900: "Tram Service",
    901: "City Tram Service",
    902: "Local Tram Service",
    903: "Regional Tram Service",
    904: "Sightseeing Tram Service",
    905: "Shuttle Tram Service",
    906: "All Tram Services",
    1000: "Water Transport Service",
    1100: "Air Service",
    1200: "Ferry Service",
    1300: "Aerial Lift Service",
    1301: "Telecabin Service",
    1302: "Cable Car Service",
    1303: "Elevator Service",
    1304: "Chair Lift Service",
    1305: "Drag Lift Service",
    1306: "Small Telecabin Service",
    1307: "All Telecabin Services",
    1400: "Funicular Service",
    1500: "Taxi Service",
    1501: "Communal Taxi Service",
    1502: "Water Taxi Service",
    1503: "Rail Taxi Service",
    1504: "Bike Taxi Service",
    1505: "Licensed Taxi Service",
    1506: "Private Hire Service Vehicle",
    1507: "All Taxi Services",
    1700: "Miscellaneous Service",
    1702: "Horse-drawn Carriage",
}


@deprecated("Use load_gtfs_delayed if possible")
def load_gtfs(
    filepath: str | Path,
    subset: None | list[str] = None,
    lazy=False,
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
    default_files = [f.file for f in REQUIRED_FILES + OPTIONAL_FILES]

    if subset is None:
        subset = []

    df_dict = GtfsDict()
    p: Path = Path(filepath)

    files_to_read = subset if only_subset else (subset + default_files)

    if not p.exists():
        raise Exception(f"{p} Does not exist")

    if p.is_dir():
        for file_name in p.iterdir():
            if file_name.is_file():
                file_key = file_name.stem
                if file_key in files_to_read:
                    df_dict[file_key] = _read_from_folder(file_name, lazy)

    elif p.suffix == ".zip":
        with ZipFile(filepath) as zip_file:
            for file_name in zip_file.namelist():
                file_key = Path(file_name).stem
                if file_key in files_to_read:
                    df_dict[file_key] = _read_from_zipped(file_name, lazy, p, zip_file)
    else:
        raise Exception(f"{p} is no directory or zipfile")

    return df_dict


def _read_from_folder(file_name, lazy) -> pd.DataFrame | dd.DataFrame:
    logging.debug(f"Reading {file_name}")
    sample_df = pd.read_csv(file_name, nrows=2)
    for col in sample_df.columns:
        if col not in DTYPES:
            logging.warning(col + " not in dtypes - using type string")
            DTYPES[col] = "string"
    return (dd if lazy else pd).read_csv(
        file_name,
        low_memory=False,
        dtype=DTYPES,
    )


def _read_from_zipped(
    file_name, lazy, p, zip_file: ZipFile
) -> pd.DataFrame | dd.DataFrame:
    """
    Read a file from a zipped GTFS feed
    :param file_name: name of the file to load (e.g. agency.txt)
    :param lazy: if dask should be used for loading
    :param p: path to the zip file
    :param zip_file: the actual (opened) zip file, or None when using lazy
    :return:
    """
    logging.debug(f"Reading {file_name}")
    with zip_file.open(file_name) as file:
        sample_df = pd.read_csv(file, engine="python", nrows=2)

        for col in sample_df.columns:
            if col not in DTYPES:
                logging.warning(col + " not in dtypes - using type string")
                DTYPES[col] = "string"
    if lazy:
        return dd.read_csv(
            f"zip://{file_name}",
            encoding="utf-8",
            low_memory=False,
            dtype=DTYPES,
            storage_options={"fo": p},
        )
    else:
        with zip_file.open(file_name) as file:
            return pd.read_csv(
                file,
                encoding="utf8",
                low_memory=False,
                dtype=DTYPES,
            )


def load_gtfs_delayed(
    filepath: str | Path,
    lazy: bool = False,
) -> DelayedGtfsDict:
    p: Path = Path(filepath)

    if not p.exists():
        raise Exception(f"{p} Does not exist")

    if p.is_dir():
        existing_files = {
            file_name.stem: file_name
            for file_name in p.iterdir()
            if file_name.is_file()
            and file_name.suffix in [".txt", ".csv"]
            and file_name
        }
    elif p.suffix == ".zip":
        with ZipFile(filepath) as zip_file:
            existing_files = {
                Path(file_name).stem: file_name
                for file_name in zip_file.namelist()
                if Path(file_name).suffix in [".txt", ".csv"] and "/" not in file_name
            }
    else:
        raise Exception(f"{p} is no directory or zipfile")

    return DelayedGtfsDict(existing_files=existing_files, base_file=p, lazy=lazy)


T = TypeVar("T")


def compute_if_necessary(*args: T) -> T:
    """
    Computes the incoming args if necessary
    :param args:
    :return: args, if args are not a dask collection, otherwise the computed args
    """
    if args is None:
        return args
    if is_dask_collection(args[0]):
        res = dd.compute(*args)
        if len(res) == 1:
            return res[0]
        return res

    if len(args) == 1:
        return args[0]
    return args


class Timer:
    def __init__(
        self, description: str = "Ran for %.2f seconds", log_level=logging.DEBUG
    ):
        self.log_level = log_level
        self.description = description

    def __enter__(self):
        self.start = time.time()

    def __exit__(self, *args):
        self.end = time.time()
        logging.log(self.log_level, self.description, self.end - self.start)
