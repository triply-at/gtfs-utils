from pathlib import Path

import shapely
import geopandas as gpd


def filter_gtfs(df_dict, filter_geometry, output, transfers=False, shapes=False):
    output_dir = Path(output)

    if isinstance(filter_geometry, list):
        geom = shapely.geometry.box(*filter_geometry)
    elif isinstance(filter_geometry, shapely.geometry.base.BaseGeometry):
        geom = filter_geometry
    else:
        raise ValueError(f"filter_geometry type {type(filter_geometry)} not supported!")

    dic = df_dict["stops"][["stop_lon", "stop_lat", "stop_id"]].compute()
    gpd_data = gpd.GeoDataFrame(
        dic["stop_id"], geometry=gpd.points_from_xy(dic.stop_lon, dic.stop_lat)
    )
    del dic

    mask = gpd_data.within(geom)
    gpd_data = gpd_data[mask]

    # filter stops.txt -
    stop_ids = gpd_data["stop_id"].values
    mask = df_dict["stops"]["stop_id"].isin(stop_ids)
    df_dict["stops"][mask].to_csv(output_dir / "stops.txt", single_file=True)

    # filter transfers.txt
    if transfers and "transfers" in df_dict:
        mask = df_dict["transfers"]["from_stop_id"].isin(stop_ids) & df_dict[
            "transfers"
        ]["to_stop_id"].isin(stop_ids)
        df_dict["transfers"] = df_dict["transfers"][mask]
        df_dict["transfers"].to_csv(output_dir / "transfers.txt", single_file=True)

    if shapes and "shapes" in df_dict:
        raise NotImplementedError()

    # filter stop_times.txt -
    mask = df_dict["stop_times"]["stop_id"].isin(stop_ids)
    unique_trip_ids = df_dict["stop_times"][mask]["trip_id"].unique()
    trip_ids = unique_trip_ids.values.compute()
    mask = df_dict["stop_times"]["trip_id"].isin(trip_ids)
    df_dict["stop_times"][mask].to_csv(output_dir / "stop_times.txt", single_file=True)

    # filter trips.txt -
    mask = df_dict["trips"]["trip_id"].isin(trip_ids)
    df_dict["trips"] = df_dict["trips"][mask]
    df_dict["trips"].to_csv(output_dir / "trips.txt", single_file=True)
    del trip_ids

    # Filter route.txt
    route_ids = df_dict["trips"]["route_id"].values
    mask = df_dict["routes"]["route_id"].isin(route_ids.compute())
    df_dict["routes"] = df_dict["routes"][mask]
    df_dict["routes"].to_csv(output_dir / "routes.txt", single_file=True)

    # Filter agency.txt
    agency_ids = df_dict["routes"]["agency_id"].values
    mask = df_dict["agency"]["agency_id"].isin(agency_ids.compute())
    df_dict["agency"] = df_dict["agency"][mask]
    df_dict["agency"].to_csv(output_dir / "agency.txt", single_file=True)
