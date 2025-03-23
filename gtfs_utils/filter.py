import dataclasses
import datetime
from typing import List, Dict, Callable

import pandas as pd
import shapely
import geopandas as gpd

from gtfs_utils.utils import GtfsDict, compute_if_necessary, Timer


@dataclasses.dataclass
class BoundsFilter:
    bounds: List[float] | shapely.geometry.base.BaseGeometry
    """Bounding box or geometry to filter by"""
    complete_trips: bool
    """Keep trips complete, even if some stops are outside bounds"""
    type = "bounds"


@dataclasses.dataclass
class RouteTypeFilter:
    route_types: List[int]
    """Route types to filter by"""
    negate: bool = False
    """Negate the filter (removes given route types)"""
    type = "route_type"


Filter = BoundsFilter | RouteTypeFilter
FilterFunction = Callable[[GtfsDict, Filter], GtfsDict]


def filter_by_bounds(gtfs: GtfsDict, filt: BoundsFilter) -> GtfsDict:
    if isinstance(filt.bounds, list):
        bounds = shapely.geometry.box(*filt.bounds)
    elif isinstance(filt.bounds, shapely.geometry.base.BaseGeometry):
        bounds = filt.bounds
    else:
        raise ValueError(f"filter_geometry type {type(filt.bounds)} not supported!")

    with Timer("Converted stops to gpd for %.2f seconds"):
        all_stops = compute_if_necessary(
            gtfs["stops"][["stop_lon", "stop_lat", "stop_id"]]
        )
        stop_gdf = gpd.GeoDataFrame(
            all_stops["stop_id"],
            geometry=gpd.points_from_xy(all_stops.stop_lon, all_stops.stop_lat),
        )
        del all_stops

    with Timer("Compute stops in bounds - %.2f seconds"):
        stop_gdf = stop_gdf[stop_gdf.within(bounds)]
        stops_in_bounds = stop_gdf["stop_id"].values
        del stop_gdf

    with Timer("Filtered stop_times and stops for %.2f seconds"):
        stop_times = gtfs["stop_times"]
        stop_times_mask = stop_times["stop_id"].isin(stops_in_bounds)
        del stops_in_bounds

        trip_ids = compute_if_necessary(stop_times[stop_times_mask]["trip_id"].unique())
        if filt.complete_trips:
            # use bigger mask to keep all stop times related to a trip if at least one stop is in bounds
            stop_times_mask = stop_times["trip_id"].isin(trip_ids)

        stop_times = stop_times[stop_times_mask]
        all_stop_ids = compute_if_necessary(stop_times["stop_id"].unique())

        stops = gtfs["stops"]
        if "parent_station" in gtfs["stops"]:
            # find parents of selected stops
            stops = stops[stops["stop_id"].isin(all_stop_ids)]
            parent_stops = compute_if_necessary(
                stops["parent_station"].dropna().unique()
            )
            all_stop_ids = pd.array(all_stop_ids.tolist() + parent_stops.tolist())

        all_trip_ids = gtfs.filter(
            "stop_times", lambda df: df["stop_id"].isin(all_stop_ids), "trip_id"
        )
        gtfs.filter("stops", lambda df: df["stop_id"].isin(all_stop_ids))

    with Timer("Removed (potential) orphans - %.2fs"):
        trips = gtfs.filter(
            "trips",
            lambda df: df["trip_id"].isin(all_trip_ids),
            ["service_id", "route_id", "shape_id"],
        )
        gtfs.filter(
            "transfers",
            lambda df: df["from_stop_id"].isin(all_stop_ids)
            | df["to_stop_id"].isin(all_stop_ids),
        )
        gtfs.filter("shapes", lambda df: df["shape_id"].isin(trips["shape_id"]))
        gtfs.filter("calendar", lambda df: df["service_id"].isin(trips["service_id"]))
        gtfs.filter(
            "calendar_dates", lambda df: df["service_id"].isin(trips["service_id"])
        )
        agency_ids = gtfs.filter(
            "routes", lambda df: df["route_id"].isin(trips["route_id"]), "agency_id"
        )
        gtfs.filter("agency", lambda df: df["agency_id"].isin(agency_ids))
        gtfs.filter("frequencies", lambda df: df["trip_id"].isin(all_trip_ids))
        fix_calendar_problems(gtfs)

    return gtfs


def fix_calendar_problems(df_dict: GtfsDict):
    if "calendar" in df_dict and "calendar_dates" in df_dict:
        calendar = compute_if_necessary(df_dict["calendar"])
        service_ids = calendar["service_id"].unique()

        mask = (~df_dict["calendar_dates"]["service_id"].isin(service_ids)) & (
            df_dict["calendar_dates"]["exception_type"] == 1
        )

        missing_calendar_date_values = df_dict["calendar_dates"][mask].groupby(
            "service_id"
        )
        for i, service_group in missing_calendar_date_values:
            records = [
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                service_group.date.min(),
                service_group.date.max(),
                i,
            ]
            for j, service in service_group.iterrows():
                day = datetime.datetime.strptime(str(service.date), "%Y%m%d").weekday()
                records[day] = 1
            calendar.loc[len(calendar)] = records
        df_dict["calendar"] = calendar


def filter_by_route_type(gtfs: GtfsDict, filt: RouteTypeFilter) -> GtfsDict:
    routes = gtfs.filter(
        "routes",
        lambda df: ~df["route_type"].isin(filt.route_types)
        if filt.negate
        else df["route_type"].isin(filt.route_types),
        ["route_id", "agency_id"],
    )

    with Timer("Removed (potential) orphans - %.2fs"):
        route_ids = routes["route_id"]
        agency_ids = routes["agency_id"]
        gtfs.filter("agency", lambda df: df["agency_id"].isin(agency_ids))
        trips = gtfs.filter(
            "trips",
            lambda df: df["route_id"].isin(route_ids),
            ["service_id", "shape_id", "trip_id"],
        )
        gtfs.filter("calendar", lambda df: df["service_id"].isin(trips["service_id"]))
        gtfs.filter(
            "calendar_dates", lambda df: df["service_id"].isin(trips["service_id"])
        )
        gtfs.filter("shapes", lambda df: df["shape_id"].isin(trips["shape_id"]))
        stop_ids = gtfs.filter(
            "stop_times", lambda df: df["trip_id"].isin(trips["trip_id"]), "stop_id"
        )
        gtfs.filter("stops", lambda df: df["stop_id"].isin(stop_ids))
        gtfs.filter(
            "transfers",
            lambda df: df["from_stop_id"].isin(stop_ids)
            | df["to_stop_id"].isin(stop_ids),
        )
        gtfs.filter("frequencies", lambda df: df["trip_id"].isin(trips["trip_id"]))
        fix_calendar_problems(gtfs)

    return gtfs


_filters: Dict[str, FilterFunction] = {
    "bounds": filter_by_bounds,
    "route_type": filter_by_route_type,
}


def filter_gtfs(df_dict: GtfsDict, filters: List[Filter]) -> GtfsDict:
    """
    Filter the gtfs feed based on the filters provided. Careful - this overwrites data in the  input GtfsDict!
    :param df_dict: gtfs feed to filter
    :param filters: list of filters to apply
    :return:
    """
    # todo: optionally we could do the orphan removal steps after all filters?
    # todo: fail fast on wrong filters

    for f in filters:
        if f.type in _filters:
            filter_function = _filters[f.type]
            df_dict = filter_function(df_dict, f)
        else:
            raise ValueError(f"Filter type {f.type} not supported!")
    return df_dict
