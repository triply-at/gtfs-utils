import dask.dataframe as dd
from pathlib import Path

__version__ = "0.0.1"

REQUIRED_GTFS_FILES = [
    "agency",
    "stops",
    "routes",
    "trips",
    "calendar",
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
                            "stop_headsign": "string",
                            "shape_id": "float64",
                            "wheelchair_accessible": "float64",
                            "stop_code": "string",
                            "route_long_name": "string",
                            "route_short_name": "string",
                            "shape_id": "category",
                        },
                    )

    else:
        raise Exception(f"{p} is no directory")

    return df_dict