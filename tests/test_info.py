import logging
from datetime import datetime

import pandas as pd
import pytest

import gtfs_utils
from gtfs_utils.utils import GtfsDict

logging.getLogger().setLevel(logging.DEBUG)


def test__load_sample_gtfs(data_dir):
    # Sample feed from:
    # https://developers.google.com/transit/gtfs/examples/gtfs-feed
    # https://developers.google.com/static/transit/gtfs/examples/sample-feed.zip
    filepath = data_dir / "sample-feed.zip"
    df_dict = GtfsDict.load(filepath, lazy=False)

    assert isinstance(df_dict, GtfsDict)
    for key in df_dict:
        assert isinstance(df_dict[key], pd.DataFrame)


@pytest.fixture(params=["sample-feed.gtfs", "sample-feed.zip"])
def sample_gtfs_path(data_dir, request):
    return data_dir / request.param


@pytest.fixture
def sample_gtfs(sample_gtfs_path, lazy):
    return gtfs_utils.load_gtfs_delayed(sample_gtfs_path, lazy=lazy)


def test__get_bounding_box(sample_gtfs):
    bbox = gtfs_utils.get_bounding_box(sample_gtfs)

    assert isinstance(bbox, tuple)
    assert len(bbox) == 4
    assert (bbox[0] <= bbox[2]) and (bbox[1] <= bbox[3])
    assert bbox == (-117.133162, 36.425288, -116.40094, 36.915682)


def test__get_calendar_date_range(sample_gtfs):
    min_date, max_date = gtfs_utils.get_calendar_date_range(sample_gtfs)

    assert isinstance(min_date, datetime) and isinstance(max_date, datetime)
    assert min_date <= max_date
    assert min_date == datetime(2007, 1, 1)
    assert max_date == datetime(2010, 12, 31)
