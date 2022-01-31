import dask.dataframe as dd
from pathlib import Path

__version__ = "0.0.1"

REQUIRED_GTFS_FILES = [
    "agency",
    "stops",
    "routes",
    "trips",
    "calendar",
    "calendar_dates",
    "stop_times",
    # 'shapes',
    # 'frequencies',
    # 'feedinfo'
]


def load_gtfs(filepath, subset=[]):
    df_dict = {}
    p = Path(filepath)
    if p.is_dir():
        for file in p.iterdir():
            if file.is_file():
                filekey = file.stem
                if filekey in REQUIRED_GTFS_FILES or filekey in subset:
                    df_dict[filekey] = dd.read_csv(
                        file,
                        low_memory=False,
                        dtype={
                            "agency_id": "UInt32",
                            "agency_name": "string",
                            "agency_url": "string",
                            "agency_timezone": "string",
                            "agency_lang": "string",
                            "agency_phone": "string",
                            "stop_headsign": "string",
                            "shape_id": "float64",
                            "shape_pt_sequence": "UInt32",
                            "shape_pt_lat": "float32",
                            "shape_pt_lon": "float32",
                            "shape_dist_traveled": "float32",
                            "stop_lat": "float32",
                            "stop_url": "string",
                            "location_type": "UInt8",
                            "monday": "UInt8",
                            "tuesday": "UInt8",
                            "wednesday": "UInt8",
                            "thursday": "UInt8",
                            "friday": "UInt8",
                            "saturday": "UInt8",
                            "sunday": "UInt8",
                            "direction_id": "UInt8",
                            "route_type": "UInt8",
                            "transfer_type": "UInt8",
                            "pickup_type": "UInt8",
                            "drop_off_type": "UInt8",
                            "min_transfer_type": "UInt8",
                            "exception_type": "UInt8",
                            "parent_station": "UInt32",
                            "stop_sequence": "UInt32",
                            "stop_lon": "float32",
                            "route_id": "UInt64",
                            "service_id": "UInt64",
                            "date": "UInt64",
                            "start_date": "UInt64",
                            "end_date": "UInt64",
                            "trip_id": "UInt64",
                            "trip_headsign": "string",
                            "arrival_time": "string",
                            "departure_time": "string",
                            "stop_id": "UInt32",
                            "from_stop_id": "UInt32",
                            "to_stop_id": "UInt32",
                            "wheelchair_accessible": "UInt8",
                            "bikes_allowed": "UInt8",
                            "stop_code": "string",
                            "stop_name": "string",
                            "route_long_name": "string",
                            "route_short_name": "string",
                        },
                    )

    else:
        raise Exception(f"{p} is no directory")

    return df_dict
