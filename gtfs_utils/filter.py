import logging
import time
from pathlib import Path
from typing import List

import numpy as np
import shapely
import geopandas as gpd

from gtfs_utils.utils import GtfsDict, compute_if_necessary


def cleanup_stop_transfers(df_dict, output_dir: Path, all_stop_ids):
    t = time.time()
    mask = df_dict["stops"]["stop_id"].isin(all_stop_ids)
    df_dict["stops"] = df_dict["stops"][mask]
    df_dict.save_file("stops", output_dir)
    duration = time.time() - t
    logging.debug(f"Filtered stops.txt for {duration:.2f}s")

    # filter transfers.txt
    if "transfers" in df_dict:
        t = time.time()
        mask = df_dict["transfers"]["from_stop_id"].isin(all_stop_ids) & df_dict[
            "transfers"
        ]["to_stop_id"].isin(all_stop_ids)
        df_dict["transfers"] = df_dict["transfers"][mask]
        df_dict.save_file("transfers", output_dir)
        duration = time.time() - t
        logging.debug(f"Filtered transfers.txt for {duration:.2f}s")


def cleanup_calendar(df_dict: GtfsDict, output_dir: Path):
    if "calendar" in df_dict or "calendar_dates" in df_dict:
        service_ids = compute_if_necessary(df_dict["trips"]["service_id"].unique())

        # filter calendar
        if "calendar" in df_dict:
            t = time.time()
            mask = df_dict["calendar"]["service_id"].isin(service_ids)

            df_dict["calendar"] = df_dict["calendar"][mask]
            df_dict.save_file("calendar", output_dir)

            duration = time.time() - t
            logging.debug(f"Filtered calendar.txt for {duration:.2f}s")

        # filter calendar dates
        if "calendar_dates" in df_dict:
            t = time.time()
            mask = df_dict["calendar_dates"]["service_id"].isin(service_ids)
            df_dict["calendar_dates"] = df_dict["calendar_dates"][mask]
            df_dict.save_file("calendar_dates", output_dir)

            duration = time.time() - t
            logging.debug(f"Filtered calendar_dates.txt for {duration:.2f}s")

        del service_ids


def remove_route_with_type(df_dict, output, types):
    output_dir = Path(output)

    mask = df_dict["routes"]["route_type"].isin(types)
    unique_route_ids = df_dict["routes"][mask]["route_id"].unique().compute()
    df_dict["routes"] = df_dict["routes"][~mask]
    df_dict["routes"].to_csv(output_dir / "routes.txt", single_file=True, index=False)

    mask = df_dict["trips"]["route_id"].isin(unique_route_ids)
    unique_trip_ids = df_dict["trips"][mask]["trip_id"].unique().compute()
    df_dict["trips"] = df_dict["trips"][~mask]
    df_dict["trips"].to_csv(output_dir / "trips.txt", single_file=True, index=False)

    del unique_route_ids

    mask = df_dict["stop_times"]["trip_id"].isin(unique_trip_ids)
    df_dict["stop_times"] = df_dict["stop_times"][~mask]
    all_stop_ids = df_dict["stop_times"]["stop_id"].unique().compute()
    df_dict["stop_times"].to_csv(
        output_dir / "stop_times.txt", single_file=True, index=False
    )

    cleanup_stop_transfers(df_dict, output_dir, all_stop_ids)
    del all_stop_ids

    cleanup_calendar(df_dict, output_dir)


def filter_gtfs(
    df_dict: GtfsDict,
    filter_geometry: List[float] | shapely.geometry.base.BaseGeometry,
    output: str | Path,
    shapes=False,
    complete_trips=False,
):
    output_dir = Path(output)

    if isinstance(filter_geometry, list):
        geom = shapely.geometry.box(*filter_geometry)
    elif isinstance(filter_geometry, shapely.geometry.base.BaseGeometry):
        geom = filter_geometry
    else:
        raise ValueError(f"filter_geometry type {type(filter_geometry)} not supported!")

    dic = compute_if_necessary(df_dict["stops"][["stop_lon", "stop_lat", "stop_id"]])

    gpd_data = gpd.GeoDataFrame(
        dic["stop_id"], geometry=gpd.points_from_xy(dic.stop_lon, dic.stop_lat)
    )
    del dic

    t = time.time()
    mask = gpd_data.within(geom)
    gpd_data = gpd_data[mask]
    stop_ids = gpd_data["stop_id"].values
    duration = time.time() - t
    logging.debug(f"Filtered stops in bounds for {duration:.2f}s")

    # filter stop_times.txt -
    t = time.time()
    mask = df_dict["stop_times"]["stop_id"].isin(stop_ids)
    del stop_ids
    unique_trip_ids = df_dict["stop_times"][mask]["trip_id"].unique()
    trip_ids = compute_if_necessary(unique_trip_ids)

    if complete_trips:
        mask = df_dict["stop_times"]["trip_id"].isin(trip_ids)

    all_stop_ids = compute_if_necessary(df_dict["stop_times"][mask]["stop_id"])
    df_dict["stop_times"] = df_dict["stop_times"][mask]
    df_dict.save_file("stop_times", output_dir)
    duration = time.time() - t
    logging.debug(f"Filtered stop_times.txt for {duration:.2f}s")

    # filter trips.txt -
    t = time.time()
    mask = df_dict["trips"]["trip_id"].isin(trip_ids)
    df_dict["trips"] = df_dict["trips"][mask]
    if not shapes:
        df_dict["trips"] = df_dict["trips"].assign(shape_id=np.nan)
    df_dict.save_file("trips", output_dir)
    del trip_ids
    duration = time.time() - t
    logging.debug(f"Filtered trips.txt for {duration:.2f}s")

    cleanup_stop_transfers(df_dict, output_dir, all_stop_ids)

    if shapes and "shapes" in df_dict:
        raise NotImplementedError()

    del all_stop_ids

    cleanup_calendar(df_dict, output_dir)

    # Filter route.txt
    t = time.time()
    route_ids = df_dict["trips"]["route_id"].unique()
    mask = df_dict["routes"]["route_id"].isin(compute_if_necessary(route_ids))
    df_dict["routes"] = df_dict["routes"][mask]
    df_dict.save_file("routes", output_dir)
    duration = time.time() - t
    logging.debug(f"Filtered routes.txt for {duration:.2f}s")

    # Filter agency.txt
    t = time.time()
    agency_ids = df_dict["routes"]["agency_id"].unique()
    mask = df_dict["agency"]["agency_id"].isin(compute_if_necessary(agency_ids))
    df_dict["agency"] = df_dict["agency"][mask]
    df_dict.save_file("agency", output_dir)
    duration = time.time() - t
    logging.debug(f"Filtered agency.txt for {duration:.2f}s")
