import json
from zipfile import ZipFile

import pytest
from typer.testing import CliRunner

import gtfs_utils.cli.patch
from gtfs_utils import apply_patch, load_gtfs_delayed, load_patch
from gtfs_utils.cli.app import app
from gtfs_utils.patch import PatchError


@pytest.fixture
def src(data_dir):
    return data_dir / "sample-feed.gtfs"


@pytest.fixture
def patch_file(data_dir):
    return data_dir / "patches" / "sample-ab.yaml"


def frequency_patch(tmp_path):
    path = tmp_path / "city.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "ops": [
                    {
                        "op": "insert_stops",
                        "select": {"route_id": "CITY"},
                        "per_direction": {
                            "0": {
                                "stop": {
                                    "stop_id": "NEW",
                                    "lat": 36.915,
                                    "lon": -116.765,
                                },
                                "between": ["NANAA", "NADAV"],
                            }
                        },
                    }
                ],
            }
        )
    )
    return path


def zip_contents(path):
    with ZipFile(path) as z:
        return {name: z.read(name) for name in sorted(z.namelist())}


def test_apply_leaves_input_unchanged(src, patch_file):
    gtfs = load_gtfs_delayed(src)
    stop_times, stops = gtfs.stop_times(), gtfs.stops()

    patched, report = apply_patch(gtfs, load_patch(patch_file))

    assert report.trips_patched == 2
    assert report.trips_skipped == 0
    assert gtfs.stop_times() is stop_times
    assert gtfs.stops() is stops
    assert len(patched.stop_times()) == len(stop_times) + 2
    assert patched.trips() is gtfs.trips()
    assert patched.specs == gtfs.specs


def test_ops_apply_in_order(src, patch_file):
    first = load_patch(patch_file)
    second = load_patch(
        {
            "version": 1,
            "ops": [
                {
                    "op": "insert_stops",
                    "select": {"route_id": "AB"},
                    "per_direction": {
                        0: {
                            "stop": {
                                "stop_id": "LATER",
                                "lat": 36.876,
                                "lon": -116.806,
                            },
                            "between": ["MIDWAY_0", "BULLFROG"],
                        }
                    },
                }
            ],
        }
    )
    combined = type(first)(version=1, ops=first.ops + second.ops)

    patched, report = apply_patch(load_gtfs_delayed(src), combined)

    st = patched.stop_times()
    assert list(st.loc[st["trip_id"] == "AB1", "stop_id"]) == [
        "BEATTY_AIRPORT",
        "MIDWAY_0",
        "LATER",
        "BULLFROG",
    ]
    assert [op.op_id for op in report.ops] == ["ab-midway", "ops[1]"]


def test_abort_in_later_op_leaves_input_unchanged(src, patch_file, tmp_path):
    gtfs = load_gtfs_delayed(src)
    stop_times = gtfs.stop_times()
    combined = load_patch(patch_file)
    combined = type(combined)(
        version=1, ops=combined.ops + load_patch(frequency_patch(tmp_path)).ops
    )

    with pytest.raises(PatchError) as e:
        apply_patch(gtfs, combined)
    assert e.value.code == "frequency_trip"
    assert gtfs.stop_times() is stop_times


def test_lazy_feed_is_rejected(src, patch_file):
    with pytest.raises(PatchError) as e:
        apply_patch(load_gtfs_delayed(src, lazy=True), load_patch(patch_file))
    assert e.value.code == "lazy_feed"


runner = CliRunner()


def test_cli_apply(src, patch_file, tmp_path):
    output = tmp_path / "out.zip"

    result = runner.invoke(
        app, ["patch", "apply", str(src), str(patch_file), "-o", str(output)]
    )

    assert result.exit_code == 0, result.output
    assert "ab-midway" in result.output
    patched = load_gtfs_delayed(output)
    assert {"MIDWAY", "MIDWAY_0", "MIDWAY_1"} <= set(patched.stops()["stop_id"])
    assert (patched.stop_times()["stop_id"] == "MIDWAY_0").sum() == 1


def test_cli_output_is_deterministic(src, patch_file, tmp_path):
    outputs = [tmp_path / "a.zip", tmp_path / "b.zip"]
    for output in outputs:
        result = runner.invoke(
            app, ["patch", "apply", str(src), str(patch_file), "-o", str(output)]
        )
        assert result.exit_code == 0, result.output

    assert zip_contents(outputs[0]) == zip_contents(outputs[1])


def test_cli_dry_run(src, patch_file, tmp_path):
    output = tmp_path / "out.zip"

    result = runner.invoke(
        app,
        ["patch", "apply", str(src), str(patch_file), "-o", str(output), "--dry-run"],
    )

    assert result.exit_code == 0, result.output
    assert "Dry run" in result.output
    assert not output.exists()


def test_cli_abort(src, tmp_path):
    output = tmp_path / "out.zip"

    result = runner.invoke(
        app,
        ["patch", "apply", str(src), str(frequency_patch(tmp_path)), "-o", str(output)],
    )

    assert result.exit_code == 1
    assert "frequency_trip" in result.output
    assert not output.exists()


def test_cli_existing_output(src, patch_file, tmp_path):
    output = tmp_path / "out.zip"
    output.write_text("")

    result = runner.invoke(
        app, ["patch", "apply", str(src), str(patch_file), "-o", str(output)]
    )
    assert result.exit_code == 2
    assert "-f" in result.output

    result = runner.invoke(
        app, ["patch", "apply", str(src), str(patch_file), "-o", str(output), "-f"]
    )
    assert result.exit_code == 0, result.output


@pytest.mark.parametrize(
    "args, message",
    [
        ([], "--output is required"),
        (["-o", "{tmp}/out.zip", "--invalid"], "No such option"),
    ],
)
def test_cli_usage_errors(src, patch_file, tmp_path, args, message):
    args = [a.replace("{tmp}", str(tmp_path)) for a in args]
    result = runner.invoke(app, ["patch", "apply", str(src), str(patch_file), *args])

    assert result.exit_code == 2
    assert message in result.output


def test_cli_invalid_patch(src, tmp_path):
    path = tmp_path / "bad.json"
    path.write_text('{"version": 2, "ops": []}')

    result = runner.invoke(app, ["patch", "apply", str(src), str(path), "--dry-run"])

    assert result.exit_code == 2
    assert "version" in result.output


def test_cli_yaml_missing(src, patch_file, monkeypatch):
    monkeypatch.setattr(gtfs_utils.cli.patch, "yaml_available", lambda: False)

    result = runner.invoke(
        app, ["patch", "apply", str(src), str(patch_file), "--dry-run"]
    )

    assert result.exit_code == 2
    assert "gtfsutils[yaml]" in result.output
