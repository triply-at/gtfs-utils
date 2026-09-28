import pandas as pd
import pytest

from gtfs_utils import GTFS, load_gtfs_delayed
from gtfs_utils.filter import BoundsFilter, filter_gtfs


@pytest.fixture
def feed(data_dir, lazy):
    return load_gtfs_delayed(data_dir / "demand-vehicles-feed.gtfs", lazy=lazy)


def test_extension_dtypes(feed):
    assert feed.demands()["demand"].dtype == "UInt32"
    assert feed.vehicles()["capacity"].dtype == "float64"
    assert feed.trips()["shift_id"].dtype == "string"
    assert feed.shifts()["shift_end"].dtype == "string"


def test_filter_removes_extension_orphans(feed):
    # only STOP_001 and STOP_002 -> route R1
    filtered = filter_gtfs(
        feed,
        [BoundsFilter(bounds=[16.36, 48.208, 16.388, 48.22], complete_trips=False)],
    )

    trips = set(pd.Series(filtered.trips()["trip_id"]).tolist())
    assert trips and all(t.startswith("R1_") for t in trips)

    demands = filtered.demands()
    assert set(demands["trip_id"]) <= trips
    assert set(demands["stop_id"]) <= {"STOP_001", "STOP_002"}
    assert set(filtered.vehicles()["trip_id"]) == trips
    assert len(filtered.shifts()) == 3


def test_base_spec_only_reads_extension_columns_as_string(data_dir):
    feed = load_gtfs_delayed(data_dir / "demand-vehicles-feed.gtfs", specs=(GTFS,))
    assert feed.demands()["demand"].dtype == "string"
