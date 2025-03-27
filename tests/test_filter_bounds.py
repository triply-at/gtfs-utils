import logging

import pandas as pd
import dask.dataframe as dd
import pytest

import gtfs_utils
from gtfs_utils.filter import BoundsFilter
from gtfs_utils.utils import GtfsDict

logging.getLogger().setLevel(logging.DEBUG)


@pytest.fixture()
def vienna_south_bounds():
    return [
        16.2,
        47.95,
        16.35,
        48.1,
    ]


@pytest.fixture()
def vienna_data_path(data_dir):
    return data_dir / "vienna.zip"


def test__filter_with_empty_bounds(vienna_data_path, lazy):
    gtfs = gtfs_utils.load_gtfs_delayed(vienna_data_path, lazy=lazy)

    assert isinstance(gtfs, GtfsDict)
    for key in gtfs:
        assert isinstance(gtfs[key], pd.DataFrame) or isinstance(
            gtfs[key], dd.DataFrame
        )

    bounds = [0, 0, 0, 0]
    filtered = gtfs_utils.filter_gtfs(
        gtfs, [BoundsFilter(bounds=bounds, complete_trips=True)]
    )

    assert isinstance(filtered, GtfsDict)
    for key in filtered:
        assert len(gtfs[key]) == 0, "every df should be empty"


@pytest.mark.parametrize("complete_trips,expected_stops", [(True, 75), (False, 30)])
def test__filter_bounds_vienna(
    vienna_data_path,
    lazy,
    vienna_south_bounds,
    complete_trips,
    expected_stops,
):
    gtfs = gtfs_utils.load_gtfs_delayed(vienna_data_path, lazy=lazy)

    filtered = gtfs_utils.filter_gtfs(
        gtfs, [BoundsFilter(bounds=vienna_south_bounds, complete_trips=complete_trips)]
    )

    assert isinstance(filtered, GtfsDict)
    for key in filtered:
        assert len(filtered[key]) > 0, "every df should be non-empty"

    assert len(gtfs.stops()) == expected_stops, f"Expected {expected_stops} stops"


def test__filter_by_own_bounds(vienna_data_path, lazy):
    """
    Filtering a gtfs feed by its own bounds -> Should return the same feed
    """
    gtfs = gtfs_utils.load_gtfs_delayed(vienna_data_path, lazy=lazy)
    file_sizes = {key: len(gtfs[key]) for key in gtfs}
    bounds = gtfs.bounds()

    filtered = gtfs_utils.filter_gtfs(
        gtfs, [BoundsFilter(bounds=list(bounds), complete_trips=False)]
    )

    for file, size in file_sizes.items():
        # delta allows configuring the tolerance for some originally included orphans that are now removed
        delta = 0

        if file == "routes":
            delta = 1

        if file == "stop_times" or file == "stops":
            delta = 5

        assert len(filtered[file]) == pytest.approx(size, delta), (
            f"Expected entries in {size} {file}"
        )
