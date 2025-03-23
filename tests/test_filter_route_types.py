import logging

import pandas as pd
import dask.dataframe as dd
import pytest

import gtfs_utils
from gtfs_utils.filter import RouteTypeFilter
from gtfs_utils.utils import GtfsDict, ROUTE_TYPES, compute_if_necessary

logging.getLogger().setLevel(logging.DEBUG)


@pytest.fixture()
def sample_data(data_dir):
    return data_dir / "sample-feed.gtfs"


def test__filter_with_all_route_types(sample_data, lazy):
    gtfs = gtfs_utils.load_gtfs(sample_data, lazy=lazy)

    file_sizes = {key: len(gtfs[key]) for key in gtfs}

    filtered = gtfs_utils.filter_gtfs(
        gtfs, [RouteTypeFilter(route_types=list(ROUTE_TYPES.keys()))]
    )

    assert isinstance(filtered, GtfsDict)

    for file, size in file_sizes.items():
        assert len(gtfs[file]) == size, f"file {file} should have {size} rows"


def test__filter_with_all_route_types_negated(sample_data, lazy):
    gtfs = gtfs_utils.load_gtfs(sample_data, lazy=lazy)

    filtered = gtfs_utils.filter_gtfs(
        gtfs, [RouteTypeFilter(route_types=ROUTE_TYPES.keys(), negate=True)]
    )

    assert isinstance(filtered, GtfsDict)

    for key in filtered:
        assert len(gtfs[key]) == 0, f"df should be empty - ({key})"


def test__filter_by_type_3(sample_data, lazy):
    gtfs = gtfs_utils.load_gtfs(sample_data, lazy=lazy)

    assert isinstance(gtfs, GtfsDict)
    for key in gtfs:
        assert isinstance(gtfs[key], pd.DataFrame) or isinstance(
            gtfs[key], dd.DataFrame
        )

    filtered = gtfs_utils.filter_gtfs(gtfs, [RouteTypeFilter(route_types=[3])])

    assert isinstance(filtered, GtfsDict)
    existing_routes = compute_if_necessary(filtered.routes()["route_id"].unique())
    assert len(existing_routes) == 4
    assert "AB" not in existing_routes
    assert len(gtfs.agency()) == 1
