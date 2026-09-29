import pandas as pd
import pytest

from gtfs_utils import load_gtfs_delayed
from gtfs_utils.patch import PatchError, load_patch
from gtfs_utils.patch.ops import insert_stops


@pytest.fixture
def sample_feed(data_dir):
    return load_gtfs_delayed(data_dir / "sample-feed.gtfs")


@pytest.fixture
def dv_feed(data_dir):
    return load_gtfs_delayed(data_dir / "demand-vehicles-feed.gtfs")


def run(feed, op: dict):
    patch = load_patch({"version": 1, "ops": [{"op": "insert_stops", **op}]})
    return insert_stops(feed, patch.ops[0], 0)


def trip(feed, trip_id) -> list[tuple]:
    st = feed.stop_times()
    rows = st[st["trip_id"] == trip_id]
    return [
        (r.stop_id, r.arrival_time, r.departure_time, int(r.stop_sequence))
        for r in rows.itertuples()
    ]


def new_stop_to_company(**entry):
    return {
        "select": {"route_id": "R1"},
        "per_direction": {
            0: {
                "stop": {"stop_id": "NEW_TO", "lat": 48.222, "lon": 16.392},
                "between": ["STOP_002", "STOP_COMPANY"],
                **entry,
            }
        },
    }


def test_sample_ab_with_station(sample_feed, data_dir):
    patch = load_patch(data_dir / "patches" / "sample-ab.yaml")
    report = insert_stops(sample_feed, patch.ops[0], 0)

    assert report.op_id == "ab-midway"
    assert report.trips_selected == 2
    assert report.trips_patched == {0: 1, 1: 1}
    assert report.skipped == []
    assert report.stops_added == ["MIDWAY", "MIDWAY_0", "MIDWAY_1"]

    # delta 0: existing times untouched, new stop inside the gap
    ab1 = trip(sample_feed, "AB1")
    assert [s[0] for s in ab1] == ["BEATTY_AIRPORT", "MIDWAY_0", "BULLFROG"]
    assert ab1[0][1:3] == ("8:00:00", "8:00:00")
    assert ab1[2][1:3] == ("8:10:00", "8:15:00")
    assert "08:00:00" < ab1[1][1] == ab1[1][2] < "08:10:00"
    assert [s[3] for s in ab1] == [1, 2, 3]

    # last_stop anchor, delta 60: departure moves earlier, arrival kept
    ab2 = trip(sample_feed, "AB2")
    assert [s[0] for s in ab2] == ["BULLFROG", "MIDWAY_1", "BEATTY_AIRPORT"]
    assert ab2[0][1:3] == ("12:04:00", "12:04:00")
    assert ab2[2][1:3] == ("12:15:00", "12:15:00")
    arrival, departure = pd.to_timedelta(ab2[1][1]), pd.to_timedelta(ab2[1][2])
    assert departure - arrival == pd.Timedelta(seconds=20)

    stops = sample_feed.stops().set_index("stop_id")
    assert stops.at["MIDWAY", "location_type"] == 1
    assert stops.at["MIDWAY", "stop_lat"] == pytest.approx(36.8747)
    assert stops.at["MIDWAY_0", "parent_station"] == "MIDWAY"
    assert stops.at["MIDWAY_1", "parent_station"] == "MIDWAY"
    assert stops.at["MIDWAY_0", "stop_name"] == "Midway (Demo)"


def test_reapply_is_skipped(sample_feed, data_dir):
    patch = load_patch(data_dir / "patches" / "sample-ab.yaml")
    insert_stops(sample_feed, patch.ops[0], 0)
    stop_times, stops = sample_feed.stop_times(), sample_feed.stops()

    report = insert_stops(sample_feed, patch.ops[0], 0)

    assert report.trips_patched == {0: 0, 1: 0}
    assert {s.code for s in report.skipped} == {"already_present"}
    assert sample_feed.stop_times() is stop_times
    assert sample_feed.stops() is stops


def test_worked_example_last_stop(dv_feed):
    report = run(
        dv_feed,
        new_stop_to_company(anchor="last_stop", delta_s=120, dwell_s=20, offset_s=300),
    )

    assert report.trips_patched == {0: 3}
    assert trip(dv_feed, "R1_NORMAL_TO") == [
        ("STOP_001", "08:28:00", "08:28:00", 1),
        ("STOP_002", "08:38:00", "08:38:00", 2),
        ("NEW_TO", "08:43:00", "08:43:20", 3),
        ("STOP_COMPANY", "08:55:00", "08:55:00", 4),
    ]
    # other routes and directions untouched
    assert trip(dv_feed, "R1_NORMAL_FROM")[0] == (
        "STOP_COMPANY",
        "17:05:00",
        "17:05:00",
        1,
    )


def test_first_stop_shifts_later(dv_feed):
    run(
        dv_feed,
        {
            "select": {"route_id": "R1"},
            "per_direction": {
                1: {
                    "stop": {"stop_id": "NEW_FROM", "lat": 48.222, "lon": 16.385},
                    "between": ["STOP_COMPANY", "STOP_001"],
                    "anchor": "first_stop",
                    "delta_s": 90,
                    "dwell_s": 20,
                    "offset_s": 480,
                }
            },
        },
    )

    assert trip(dv_feed, "R1_NORMAL_FROM") == [
        ("STOP_COMPANY", "17:05:00", "17:05:00", 1),
        ("NEW_FROM", "17:13:00", "17:13:20", 2),
        ("STOP_001", "17:21:30", "17:21:30", 3),
        ("STOP_002", "17:31:30", "17:31:30", 4),
    ]
    assert trip(dv_feed, "R1_NIGHT_FROM")[-1][1] == "30:31:30"


def test_new_row_follows_a(dv_feed):
    run(dv_feed, new_stop_to_company())

    st = dv_feed.stop_times()
    first = st.index[st["trip_id"] == "R1_EARLY_TO"]
    assert list(st.loc[first, "stop_id"]) == [
        "STOP_001",
        "STOP_002",
        "NEW_TO",
        "STOP_COMPANY",
    ]
    assert list(first) == list(range(first[0], first[0] + 4))
    assert len(st) == 36 + 3


def test_zone_from_preceding_stop(dv_feed):
    stops = dv_feed.stops()
    stops["zone_id"] = pd.array(["Z1", "Z2", "Z3", "Z4", "Z_COMPANY"], dtype="string")
    dv_feed["stops"] = stops

    run(dv_feed, new_stop_to_company())

    assert dv_feed.stops().set_index("stop_id").at["NEW_TO", "zone_id"] == "Z2"


def test_sequence_gap_is_used(dv_feed):
    st = dv_feed.stop_times()
    st.loc[st["stop_id"] == "STOP_COMPANY", "stop_sequence"] = 10
    dv_feed["stop_times"] = st

    run(dv_feed, new_stop_to_company())

    assert [s[3] for s in trip(dv_feed, "R1_EARLY_TO")] == [1, 2, 3, 10]


def test_shift_selector(dv_feed):
    report = run(
        dv_feed,
        {**new_stop_to_company(), "select": {"route_id": "R1", "shift_id": "EARLY"}},
    )

    assert report.trips_selected == 2
    assert report.trips_patched == {0: 1}
    assert "NEW_TO" in [s[0] for s in trip(dv_feed, "R1_EARLY_TO")]
    assert "NEW_TO" not in [s[0] for s in trip(dv_feed, "R1_NORMAL_TO")]


def test_not_adjacent(dv_feed):
    stop_times = dv_feed.stop_times()
    report = run(dv_feed, new_stop_to_company(between=["STOP_001", "STOP_COMPANY"]))

    assert report.trips_patched == {0: 0}
    assert len(report.skipped) == 3
    assert {s.code for s in report.skipped} == {"not_adjacent"}
    assert dv_feed.stop_times() is stop_times
    assert report.stops_added == []
    assert "NEW_TO" not in set(dv_feed.stops()["stop_id"])


def test_untimed(dv_feed):
    run(dv_feed, new_stop_to_company(anchor="untimed"))

    st = dv_feed.stop_times()
    new = st[st["stop_id"] == "NEW_TO"]
    assert new["arrival_time"].isna().all()
    assert new["departure_time"].isna().all()
    assert (new["timepoint"] == 0).all()
    assert trip(dv_feed, "R1_EARLY_TO")[1][1] == "05:40:00"


def test_offset_too_large(dv_feed):
    report = run(dv_feed, new_stop_to_company(offset_s=10_000))

    assert report.trips_patched == {0: 0}
    assert {s.code for s in report.skipped} == {"offset_too_large"}


def test_reuse_existing_stop_nearby(dv_feed):
    stops = dv_feed.stops()
    stops = pd.concat(
        [
            stops,
            pd.DataFrame(
                [{"stop_id": "NEW_TO", "stop_lat": 48.2221, "stop_lon": 16.392}]
            ),
        ],
        ignore_index=True,
    )
    dv_feed["stops"] = stops

    report = run(dv_feed, new_stop_to_company())

    assert report.trips_patched == {0: 3}
    assert report.stops_added == []
    assert (dv_feed.stops()["stop_id"] == "NEW_TO").sum() == 1


@pytest.mark.parametrize(
    "op, code",
    [
        (new_stop_to_company(anchor="last_stop", delta_s=6 * 3600), "negative_time"),
        ({**new_stop_to_company(), "select": {"route_id": "R9"}}, "empty_selection"),
        (
            new_stop_to_company(stop={"stop_id": "STOP_001", "lat": 48.3, "lon": 16.3}),
            "stop_conflict",
        ),
        (new_stop_to_company(stop={"stop_id": "UNKNOWN"}), "stop_missing"),
    ],
)
def test_abort_leaves_feed_unchanged(dv_feed, op, code):
    stop_times, stops = dv_feed.stop_times(), dv_feed.stops()

    with pytest.raises(PatchError) as e:
        run(dv_feed, op)

    assert e.value.code == code
    assert dv_feed.stop_times() is stop_times
    assert dv_feed.stops() is stops


def test_station_conflict(dv_feed):
    op = {**new_stop_to_company(), "station": {"stop_id": "STOP_001"}}

    with pytest.raises(PatchError) as e:
        run(dv_feed, op)
    assert e.value.code == "stop_conflict"


@pytest.mark.parametrize("location_type, ok", [(pd.NA, False), (0, False), (1, True)])
def test_existing_station(dv_feed, location_type, ok):
    stops = dv_feed.stops()
    stops["location_type"] = pd.array([pd.NA] * len(stops), dtype="UInt8")
    extra = pd.DataFrame(
        [{"stop_id": "HUB", "stop_lat": 48.22, "stop_lon": 16.39}]
    ).assign(location_type=pd.array([location_type], dtype="UInt8"))
    dv_feed["stops"] = pd.concat([stops, extra], ignore_index=True)
    op = {**new_stop_to_company(), "station": {"stop_id": "HUB"}}

    if ok:
        report = run(dv_feed, op)
        assert report.stops_added == ["NEW_TO"]
        new = dv_feed.stops().set_index("stop_id").loc["NEW_TO"]
        assert new["parent_station"] == "HUB"
        assert new["location_type"] == 0
    else:
        with pytest.raises(PatchError) as e:
            run(dv_feed, op)
        assert e.value.code == "stop_conflict"


def test_frequency_trips(sample_feed):
    op = {
        "select": {"route_id": "CITY"},
        "per_direction": {
            0: {
                "stop": {"stop_id": "NEW", "lat": 36.915, "lon": -116.765},
                "between": ["NANAA", "NADAV"],
            }
        },
    }
    with pytest.raises(PatchError) as e:
        run(sample_feed, op)
    assert e.value.code == "frequency_trip"


def test_shift_selector_without_extension(sample_feed):
    op = {
        "select": {"route_id": "AB", "shift_id": "EARLY"},
        "per_direction": {
            0: {
                "stop": {"stop_id": "NEW", "lat": 36.87, "lon": -116.80},
                "between": ["BEATTY_AIRPORT", "BULLFROG"],
            }
        },
    }
    with pytest.raises(PatchError) as e:
        run(sample_feed, op)
    assert e.value.code == "extension_missing"
