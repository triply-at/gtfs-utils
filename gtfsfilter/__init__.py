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
                            "agency_id": "uint32",
                            "agency_name": "string",
                            "agency_url": "string",
                            "agency_timezone": "string",
                            "agency_lang": "string",
                            "agency_phone": "string",
                            "stop_headsign": "string",
                            "shape_id": "float64",
                            "shape_pt_sequence": "uint32",
                            "shape_pt_lat": "float32",
                            "shape_pt_lon": "float32",
                            "shape_dist_traveled": "float32",
                            "stop_lat": "float32",
                            "stop_url": "string",
                            "location_type": "uint8",
                            "monday": "uint8",
                            "tuesday": "uint8",
                            "wednesday": "uint8",
                            "thursday": "uint8",
                            "friday": "uint8",
                            "saturday": "uint8",
                            "sunday": "uint8",
                            "direction_id": "uint8",
                            "route_type": "uint8",
                            "transfer_type": "uint8",
                            "pickup_type": "uint8",
                            "drop_off_type": "uint8",
                            "min_transfer_type": "uint8",
                            "exception_type": "uint8",
                            "parent_station": "uint32",
                            "stop_sequence": "uint32",
                            "stop_lon": "float32",
                            "route_id": "uint64",
                            "service_id": "uint64",
                            "date": "uint64",
                            "start_date": "uint64",
                            "end_date": "uint64",
                            "trip_id": "uint64",
                            "trip_headsign": "string",
                            "arrival_time": "string",
                            "departure_time": "string",
                            "stop_id": "uint32",
                            "from_stop_id": "uint32",
                            "to_stop_id": "uint32",
                            "wheelchair_accessible": "uint8",
                            "bikes_allowed": "uint8",
                            "stop_code": "string",
                            "stop_name": "string",
                            "route_long_name": "string",
                            "route_short_name": "string",
                        },
                    )

    else:
        raise Exception(f"{p} is no directory")

    return df_dict