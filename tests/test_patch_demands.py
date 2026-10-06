import pandas as pd
import pytest

from gtfs_utils import load_gtfs_delayed
from gtfs_utils.patch import PatchError, PatchLoadError, load_patch
from gtfs_utils.patch.integrity import check_foreign_keys
from gtfs_utils.patch.ops import insert_stops


@pytest.fixture
def dv_feed(data_dir):
    return load_gtfs_delayed(data_dir / "demand-vehicles-feed.gtfs")


def run(feed, **entry):
    patch = load_patch(
        {
            "version": 1,
            "ops": [
                {
                    "op": "insert_stops",
                    "select": {"route_id": "R1"},
                    "per_direction": {
                        0: {
                            "stop": {"stop_id": "NEW_TO", "lat": 48.222, "lon": 16.392},
                            "between": ["STOP_002", "STOP_COMPANY"],
                            "offset_s": 300,
                            **entry,
                        }
                    },
                }
            ],
        }
    )
    return insert_stops(feed, patch.ops[0], 0)


def test_demand_rows(dv_feed):
    original = dv_feed.demands()

    report = run(
        dv_feed, demand={"value": 3, "by_shift": {"NIGHT": 1}, "note": "survey"}
    )

    assert report.demands_added == 3
    demands = dv_feed.demands()
    pd.testing.assert_frame_equal(demands.iloc[: len(original)], original)

    new = demands[demands["stop_id"] == "NEW_TO"].set_index("trip_id")
    assert set(new.index) == {"R1_EARLY_TO", "R1_NORMAL_TO", "R1_NIGHT_TO"}
    assert new.at["R1_NORMAL_TO", "demand"] == 3
    assert new.at["R1_NIGHT_TO", "demand"] == 1
    # departure 08:40 + 300 s, default window -600 / +300 s
    assert new.at["R1_NORMAL_TO", "earliest_time"] == "08:35:00"
    assert new.at["R1_NORMAL_TO", "latest_time"] == "08:50:00"
    assert (new["demand_note"] == "survey").all()
    assert demands["demand"].dtype == original["demand"].dtype


def test_custom_window(dv_feed):
    run(dv_feed, demand={"value": 2, "window_s": [-60, 60]})

    demands = dv_feed.demands()
    new = demands[demands["trip_id"] == "R1_EARLY_TO"].set_index("stop_id")
    assert new.at["NEW_TO", "earliest_time"] == "05:44:00"
    assert new.at["NEW_TO", "latest_time"] == "05:46:00"
    assert pd.isna(new.at["NEW_TO", "demand_note"])


def test_no_demand_block_leaves_demands(dv_feed):
    demands = dv_feed.demands()
    report = run(dv_feed)

    assert report.demands_added == 0
    assert dv_feed.demands() is demands


def test_demand_exists(dv_feed):
    demands = dv_feed.demands()
    dv_feed["demands"] = pd.concat(
        [demands, pd.DataFrame([{"trip_id": "R1_EARLY_TO", "stop_id": "NEW_TO"}])],
        ignore_index=True,
    )
    stop_times = dv_feed.stop_times()

    with pytest.raises(PatchError) as e:
        run(dv_feed, demand={"value": 1})
    assert e.value.code == "demand_exists"
    assert dv_feed.stop_times() is stop_times


def test_demand_without_extension(data_dir):
    feed = load_gtfs_delayed(data_dir / "sample-feed.gtfs")
    patch = load_patch(
        {
            "version": 1,
            "ops": [
                {
                    "op": "insert_stops",
                    "select": {"route_id": "AB"},
                    "per_direction": {
                        0: {
                            "stop": {"stop_id": "NEW", "lat": 36.87, "lon": -116.80},
                            "between": ["BEATTY_AIRPORT", "BULLFROG"],
                            "demand": {"value": 1},
                        }
                    },
                }
            ],
        }
    )
    with pytest.raises(PatchError) as e:
        insert_stops(feed, patch.ops[0], 0)
    assert e.value.code == "extension_missing"


def test_demand_with_untimed_is_invalid(dv_feed):
    with pytest.raises(PatchLoadError, match="demand"):
        run(dv_feed, anchor="untimed", demand={"value": 1})


def test_foreign_keys_hold_after_patch(dv_feed, data_dir):
    baseline = load_gtfs_delayed(data_dir / "demand-vehicles-feed.gtfs")
    run(dv_feed, demand={"value": 3})

    check_foreign_keys(dv_feed, baseline)

    sizes = {f: len(dv_feed[f]) for f in dv_feed}
    dv_feed.remove_orphans()
    assert {f: len(dv_feed[f]) for f in dv_feed} == sizes


def test_foreign_key_broken_by_patch(dv_feed, data_dir):
    baseline = load_gtfs_delayed(data_dir / "demand-vehicles-feed.gtfs")
    demands = dv_feed.demands()
    dv_feed["demands"] = pd.concat(
        [demands, pd.DataFrame([{"trip_id": "R1_EARLY_TO", "stop_id": "GHOST"}])],
        ignore_index=True,
    )

    with pytest.raises(PatchError) as e:
        check_foreign_keys(dv_feed, baseline)
    assert e.value.code == "foreign_key"
    assert "demands.stop_id" in str(e.value)
    assert "GHOST" in str(e.value)


def test_foreign_key_already_broken_is_ignored(dv_feed):
    stop_times = dv_feed.stop_times()
    dv_feed["stop_times"] = pd.concat(
        [stop_times, pd.DataFrame([{"trip_id": "R1_EARLY_TO", "stop_id": "GHOST"}])],
        ignore_index=True,
    )

    check_foreign_keys(dv_feed, dv_feed)
    with pytest.raises(PatchError, match="stop_times.stop_id"):
        check_foreign_keys(dv_feed)
