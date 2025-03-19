import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import TypeVar, Mapping, Callable, List, Literal
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
    TRANSFERS = "transfers", False


class GtfsDict(dict, Mapping[str, pd.DataFrame | dd.DataFrame]):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__()

    @classmethod
    def load(cls, *args, **kwargs) -> "GtfsDict":
        return load_gtfs(*args, **kwargs)

    def save_file(self, file: str, output_dir: Path) -> None:
        if file not in self:
            raise KeyError(f"{file} not found in GTFS data")
        df = self[file]
        save_kwargs = (
            {"single_file": True, "index": False}
            if is_dask_collection(df)
            else {"index": False}
        )
        self[file].to_csv(output_dir / f"{file}.txt", **save_kwargs)

    def save(self, output_dir: Path) -> None:
        if not output_dir.exists():
            output_dir.mkdir(parents=True)
        for file in self:
            self.save_file(file, output_dir)

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
            return pd.DataFrame(columns=return_cols) if return_cols else None

        mask = where(self[file])
        self[file] = self[file][mask]  # noqa
        return compute_if_necessary(self[file][return_cols]) if return_cols else None

    def bounds(self) -> tuple[float, float, float, float]:
        from gtfs_utils.info import get_bounding_box

        return get_bounding_box(self)


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
    # extended route types are currently not supported.
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

    df_dict = GtfsDict()
    p: Path = Path(filepath)

    files_to_read = (
        subset if only_subset else (subset + [file.file for file in REQUIRED_FILES])
    )

    if not p.exists():
        raise Exception(f"{p} Does not exist")

    if p.is_dir():
        for file_name in p.iterdir():
            if file_name.is_file():
                file_key = file_name.stem
                if file_key in files_to_read:
                    logging.debug(f"Reading {file_key}")
                    sample_df = pd.read_csv(file_name, nrows=2)

                    for col in sample_df.columns:
                        if col not in DTYPES:
                            logging.warning(col + " not in dtypes - using type string")
                            DTYPES[col] = "string"

                    df_dict[file_key] = (dd if lazy else pd).read_csv(
                        file_name,
                        low_memory=False,
                        dtype=DTYPES,
                    )

    elif p.suffix == ".zip":
        with ZipFile(filepath) as zip_file:
            for file_name in zip_file.namelist():
                file_key = Path(file_name).stem
                if file_key in files_to_read:
                    logging.debug(f"Reading {file_key}")
                    with zip_file.open(file_name) as file:
                        sample_df = pd.read_csv(file, encoding="utf8", nrows=2)

                        for col in sample_df.columns:
                            if col not in DTYPES:
                                logging.warning(
                                    col + " not in dtypes - using type string"
                                )
                                DTYPES[col] = "string"
                    if lazy:
                        df_dict[file_key] = dd.read_csv(
                            f"zip://{file_name}",
                            encoding="utf8",
                            low_memory=False,
                            dtype=DTYPES,
                            storage_options={"fo": p},
                        )
                    else:
                        with zip_file.open(file_name) as file:
                            df_dict[file_key] = pd.read_csv(
                                file,
                                encoding="utf8",
                                low_memory=False,
                                dtype=DTYPES,
                            )
    else:
        raise Exception(f"{p} is no directory or zipfile")

    return df_dict


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
