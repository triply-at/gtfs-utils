import builtins
import copy

import pytest

from gtfs_utils.patch import PatchLoadError, load_patch
from gtfs_utils.patch.model import DEFAULT_DEMAND_WINDOW_S


@pytest.fixture
def patches_dir(data_dir):
    return data_dir / "patches"


def minimal_patch(**entry):
    return {
        "version": 1,
        "ops": [
            {
                "op": "insert_stops",
                "select": {"route_id": "AB"},
                "per_direction": {
                    0: {
                        "stop": {"stop_id": "NEW", "lat": 36.87, "lon": -116.80},
                        "between": ["BEATTY_AIRPORT", "BULLFROG"],
                        **entry,
                    }
                },
            }
        ],
    }


def test_load_minimal():
    patch = load_patch(minimal_patch())

    op = patch.ops[0]
    assert op.select.route_id == ("AB",)
    assert op.select.direction_id is None
    assert op.station is None

    entry = op.directions[0]
    assert entry.direction_id == 0
    assert entry.between == ("BEATTY_AIRPORT", "BULLFROG")
    assert entry.anchor is None
    assert (entry.delta_s, entry.dwell_s, entry.offset_s) == (0, 0, None)
    assert entry.demand is None


def test_yaml_and_json_are_equal(patches_dir):
    from_yaml = load_patch(patches_dir / "sample-ab.yaml")
    from_json = load_patch(patches_dir / "sample-ab.json")

    assert from_yaml == from_json
    op = from_yaml.ops[0]
    assert op.id == "ab-midway"
    assert op.station.stop_id == "MIDWAY"
    assert op.station.attributes == {"stop_name": "Midway (Demo)"}
    assert [d.direction_id for d in op.directions] == [0, 1]
    assert op.directions[0].stop.attributes == {"stop_name": "Midway (Demo)"}
    assert op.directions[1].anchor == "last_stop"
    assert op.directions[1].delta_s == 60


def test_selector_lists():
    data = minimal_patch()
    data["ops"][0]["select"] = {
        "route_id": ["AB", "BFC"],
        "direction_id": [0, 1],
        "service_id": "FULLW",
        "shift_id": ["EARLY"],
    }
    select = load_patch(data).ops[0].select

    assert select.route_id == ("AB", "BFC")
    assert select.direction_id == (0, 1)
    assert select.service_id == ("FULLW",)
    assert select.trip_id is None
    assert select.shift_id == ("EARLY",)


def test_demand():
    demand = (
        load_patch(minimal_patch(demand={"value": 3, "by_shift": {"NIGHT": 1}}))
        .ops[0]
        .directions[0]
        .demand
    )

    assert demand.window_s == DEFAULT_DEMAND_WINDOW_S
    assert demand.for_shift("NIGHT") == 1
    assert demand.for_shift("EARLY") == 3
    assert demand.for_shift(None) == 3


def test_anchor_with_delta():
    entry = (
        load_patch(minimal_patch(anchor="first_stop", delta_s=75)).ops[0].directions[0]
    )
    assert (entry.anchor, entry.delta_s) == ("first_stop", 75)


def test_untimed_without_delta():
    entry = load_patch(minimal_patch(anchor="untimed", delta_s=0)).ops[0].directions[0]
    assert entry.anchor == "untimed"


def _with(path, value):
    data = minimal_patch()
    target = data
    for key in path[:-1]:
        target = target[key]
    if value is _DELETE:
        del target[path[-1]]
    else:
        target[path[-1]] = value
    return data


_DELETE = object()
_ENTRY = ("ops", 0, "per_direction", 0)


@pytest.mark.parametrize(
    "data, message",
    [
        (_with(("version",), 2), "version"),
        (_with(("ops",), []), "ops"),
        (_with(("typo",), 1), "unknown key(s) typo"),
        (_with(("ops", 0, "op"), "remove_stops"), "insert_stops"),
        (_with(("ops", 0, "select", "route_id"), 12), "quote it in YAML"),
        (_with(("ops", 0, "select", "route_id"), _DELETE), "missing key(s) route_id"),
        (_with(("ops", 0, "select", "direction_id"), 2), "0 or 1"),
        (_with(("ops", 0, "per_direction"), {}), "must not be empty"),
        (_with(("ops", 0, "per_direction", 0, "anchr"), "first_stop"), "anchr"),
        (_with((*_ENTRY, "between"), ["BULLFROG"]), "two stop_ids"),
        (_with((*_ENTRY, "between"), ["BULLFROG", "BULLFROG"]), "must differ"),
        (_with((*_ENTRY, "between"), ["NEW", "BULLFROG"]), "new stop"),
        (_with((*_ENTRY, "anchor"), "middle"), "must be one of"),
        (_with((*_ENTRY, "delta_s"), 60), "required when delta_s > 0"),
        (_with((*_ENTRY, "delta_s"), -1), ">= 0"),
        (_with((*_ENTRY, "delta_s"), True), "integer"),
        (_with((*_ENTRY, "stop", "lon"), _DELETE), "lat and lon"),
        (_with((*_ENTRY, "stop", "lat"), 95), "out of range"),
        (_with((*_ENTRY, "demand"), {"value": 1, "window_s": [300, -600]}), "before"),
        (_with(("ops", 0, "travel_time"), {"mode": "resolved"}), "fixed"),
    ],
)
def test_invalid(data, message):
    with pytest.raises(PatchLoadError) as e:
        load_patch(data)
    assert e.value.code == "schema_invalid"
    assert message in str(e.value)


def test_untimed_with_delta():
    with pytest.raises(PatchLoadError, match="untimed"):
        load_patch(minimal_patch(anchor="untimed", delta_s=60))


def test_duplicate_new_stop():
    data = minimal_patch()
    per_direction = data["ops"][0]["per_direction"]
    per_direction[1] = copy.deepcopy(per_direction[0])
    per_direction[1]["between"] = ["BULLFROG", "BEATTY_AIRPORT"]

    with pytest.raises(PatchLoadError, match="own new stop_id"):
        load_patch(data)


def test_station_id_equals_child():
    data = minimal_patch()
    data["ops"][0]["station"] = {"stop_id": "NEW"}

    with pytest.raises(PatchLoadError, match="must differ"):
        load_patch(data)


def test_error_contains_file_and_field(tmp_path):
    path = tmp_path / "bad.json"
    path.write_text('{"version": 1, "ops": [{"op": "insert_stops"}]}')

    with pytest.raises(PatchLoadError) as e:
        load_patch(path)
    assert str(path) in str(e.value)
    assert "ops[0]" in str(e.value)


@pytest.mark.parametrize(
    "name, content, code",
    [
        ("patch.txt", "", "load_failed"),
        ("patch.json", "{", "load_failed"),
        ("patch.yaml", "a: [", "load_failed"),
    ],
)
def test_unreadable(tmp_path, name, content, code):
    path = tmp_path / name
    path.write_text(content)

    with pytest.raises(PatchLoadError) as e:
        load_patch(path)
    assert e.value.code == code


def test_missing_file(tmp_path):
    with pytest.raises(PatchLoadError) as e:
        load_patch(tmp_path / "missing.yaml")
    assert e.value.code == "load_failed"


def test_yaml_missing(patches_dir, monkeypatch):
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "yaml":
            raise ImportError
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    with pytest.raises(PatchLoadError) as e:
        load_patch(patches_dir / "sample-ab.yaml")
    assert e.value.code == "yaml_missing"
    assert "gtfsutils[yaml]" in str(e.value)

    assert load_patch(patches_dir / "sample-ab.json").ops[0].id == "ab-midway"
