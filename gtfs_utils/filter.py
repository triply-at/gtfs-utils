import dataclasses
from typing import List

import shapely
import geopandas as gpd

from gtfs_utils.utils import GtfsDict, compute_if_necessary, Timer

#
# def cleanup_stop_transfers(df_dict, output_dir: Path, all_stop_ids):
#     t = time.time()
#     mask = df_dict["stops"]["stop_id"].isin(all_stop_ids)
#     df_dict["stops"] = df_dict["stops"][mask]
#     df_dict.save_file("stops", output_dir)
#     duration = time.time() - t
#     logging.debug(f"Filtered stops.txt for {duration:.2f}s")
#
#     # filter transfers.txt
#     if "transfers" in df_dict:
#         t = time.time()
#         mask = df_dict["transfers"]["from_stop_id"].isin(all_stop_ids) & df_dict[
#             "transfers"
#         ]["to_stop_id"].isin(all_stop_ids)
#         df_dict["transfers"] = df_dict["transfers"][mask]
#         df_dict.save_file("transfers", output_dir)
#         duration = time.time() - t
#         logging.debug(f"Filtered transfers.txt for {duration:.2f}s")
#
#
# def cleanup_calendar(df_dict: GtfsDict, output_dir: Path):
#     if "calendar" in df_dict or "calendar_dates" in df_dict:
#         service_ids = compute_if_necessary(df_dict["trips"]["service_id"].unique())
#
#         # filter calendar
#         if "calendar" in df_dict:
#             t = time.time()
#             mask = df_dict["calendar"]["service_id"].isin(service_ids)
#
#             df_dict["calendar"] = df_dict["calendar"][mask]
#             df_dict.save_file("calendar", output_dir)
#
#             duration = time.time() - t
#             logging.debug(f"Filtered calendar.txt for {duration:.2f}s")
#
#         # filter calendar dates
#         if "calendar_dates" in df_dict:
#             t = time.time()
#             mask = df_dict["calendar_dates"]["service_id"].isin(service_ids)
#             df_dict["calendar_dates"] = df_dict["calendar_dates"][mask]
#             df_dict.save_file("calendar_dates", output_dir)
#
#             duration = time.time() - t
#             logging.debug(f"Filtered calendar_dates.txt for {duration:.2f}s")
#
#         del service_ids
#
#
# def remove_route_with_type(df_dict, output, types):
#     output_dir = Path(output)
#
#     mask = df_dict["routes"]["route_type"].isin(types)
#     unique_route_ids = df_dict["routes"][mask]["route_id"].unique().compute()
#     df_dict["routes"] = df_dict["routes"][~mask]
#     df_dict["routes"].to_csv(output_dir / "routes.txt", single_file=True, index=False)
#
#     mask = df_dict["trips"]["route_id"].isin(unique_route_ids)
#     unique_trip_ids = df_dict["trips"][mask]["trip_id"].unique().compute()
#     df_dict["trips"] = df_dict["trips"][~mask]
#     df_dict["trips"].to_csv(output_dir / "trips.txt", single_file=True, index=False)
#
#     del unique_route_ids
#
#     mask = df_dict["stop_times"]["trip_id"].isin(unique_trip_ids)
#     df_dict["stop_times"] = df_dict["stop_times"][~mask]
#     all_stop_ids = df_dict["stop_times"]["stop_id"].unique().compute()
#     df_dict["stop_times"].to_csv(
#         output_dir / "stop_times.txt", single_file=True, index=False
#     )
#
#     cleanup_stop_transfers(df_dict, output_dir, all_stop_ids)
#     del all_stop_ids
#
#     cleanup_calendar(df_dict, output_dir)


@dataclasses.dataclass
class BoundsFilter:
    bounds: List[float] | shapely.geometry.base.BaseGeometry
    complete_trips: bool
    type = "bounds"


@dataclasses.dataclass
class RouteTypeFilter:
    route_types: List[int]
    type = "route_type"


Filter = BoundsFilter


def apply_bounds_filter(gtfs: GtfsDict, filt: BoundsFilter) -> GtfsDict:
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
            # use bigger mask to keep all stop times related to a trip if at lease one stop is in bounds
            stop_times_mask = stop_times["trip_id"].isin(trip_ids)

        stop_times = stop_times[stop_times_mask]
        all_stop_ids = compute_if_necessary(stop_times["stop_id"].unique())
        all_trip_ids = compute_if_necessary(stop_times["trip_id"].unique())
        gtfs["stop_times"] = stop_times
        gtfs["stops"] = gtfs.filter(
            "stops", lambda df: df["stop_id"].isin(all_stop_ids)
        )

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

    return gtfs


def apply_filter(df_dict: GtfsDict, filt: Filter) -> GtfsDict:
    if filt.type == "bounds":
        return apply_bounds_filter(df_dict, filt)
    elif filt.type == "route_type":
        # todo route type filter
        raise NotImplementedError
        # return apply_route_type_filter(df_dict, filt)
    else:
        raise ValueError(f"Filter type {filt.type} not supported!")


def do_filter(df_dict: GtfsDict, filters: List[Filter]) -> GtfsDict:
    """
    Filter the gtfs feed based on the filters provided
    :param df_dict: gtfs feed to filter
    :param filters:
    :return:
    """
    # todo: optionally we could do the orphan removal steps after all filters?
    # todo: fail fast on wrong filters

    for filt in filters:
        df_dict = apply_filter(df_dict, filt)

    return df_dict


# def filter_gtfs(
#     df_dict: GtfsDict,
#     filter_geometry: List[float] | shapely.geometry.base.BaseGeometry,
#     output: str | Path,
#     shapes=False,
#     complete_trips=False,
# ):
#     output_dir = Path(output)
#
#     if isinstance(filter_geometry, list):
#         geom = shapely.geometry.box(*filter_geometry)
#     elif isinstance(filter_geometry, shapely.geometry.base.BaseGeometry):
#         geom = filter_geometry
#     else:
#         raise ValueError(f"filter_geometry type {type(filter_geometry)} not supported!")
#
#     with Timer("Converted stops to gpd for %.2f seconds"):
#         all_stops = compute_if_necessary(
#             df_dict["stops"][["stop_lon", "stop_lat", "stop_id"]]
#         )
#
#         stop_gpd = gpd.GeoDataFrame(
#             all_stops["stop_id"],
#             geometry=gpd.points_from_xy(all_stops.stop_lon, all_stops.stop_lat),
#         )
#         del all_stops
#
#     with Timer("Filter stops in bounds - %.2f seconds"):
#         mask = stop_gpd.within(geom)
#         stop_gpd = stop_gpd[mask]
#         stop_ids = stop_gpd["stop_id"].values
#
#     with Timer("Filtered stop_times.txt for %.2f seconds"):
#         mask = df_dict["stop_times"]["stop_id"].isin(stop_ids)
#         del stop_ids
#         unique_trip_ids = df_dict["stop_times"][mask]["trip_id"].unique()
#         trip_ids = compute_if_necessary(unique_trip_ids)
#
#         if complete_trips:
#             mask = df_dict["stop_times"]["trip_id"].isin(trip_ids)
#
#         all_stop_ids = compute_if_necessary(df_dict["stop_times"][mask]["stop_id"])
#         df_dict["stop_times"] = df_dict["stop_times"][mask]
#         df_dict.save_file("stop_times", output_dir)
#
#     with Timer("Filtered trips.txt for %.2f seconds"):
#         mask = df_dict["trips"]["trip_id"].isin(trip_ids)
#         df_dict["trips"] = df_dict["trips"][mask]
#         if not shapes:
#             df_dict["trips"] = df_dict["trips"].assign(shape_id=np.nan)
#         df_dict.save_file("trips", output_dir)
#         del trip_ids
#
#     cleanup_stop_transfers(df_dict, output_dir, all_stop_ids)
#
#     if shapes and "shapes" in df_dict:
#         raise NotImplementedError()
#
#     del all_stop_ids
#
#     cleanup_calendar(df_dict, output_dir)
#
#     with Timer("Filtered routes.txt for %.2f seconds"):
#         route_ids = df_dict["trips"]["route_id"].unique()
#         mask = df_dict["routes"]["route_id"].isin(compute_if_necessary(route_ids))
#         df_dict["routes"] = df_dict["routes"][mask]
#         df_dict.save_file("routes", output_dir)
#
#     with Timer("Filtered agency.txt for %.2f seconds"):
#         agency_ids = df_dict["routes"]["agency_id"].unique()
#         mask = df_dict["agency"]["agency_id"].isin(compute_if_necessary(agency_ids))
#         df_dict["agency"] = df_dict["agency"][mask]
#         df_dict.save_file("agency", output_dir)
