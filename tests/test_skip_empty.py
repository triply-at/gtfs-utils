import logging

import click
import pytest
from typer.testing import CliRunner

from gtfs_utils.cli.app import app

logging.getLogger().setLevel(logging.DEBUG)

runner = CliRunner()

EMPTY_BOUNDS = "[0, 0, 0, 0]"
NON_EMPTY_BOUNDS = "[-118, 36, -116, 37]"


@pytest.fixture()
def sample_gtfs_path(data_dir):
    return data_dir / "sample-feed.zip"


def run_filter(sample_gtfs_path, output, bounds, lazy, *options):
    args = [
        "filter",
        str(sample_gtfs_path),
        "--bounds",
        bounds,
        "--output",
        str(output),
        *options,
    ]
    if lazy:
        args.append("--lazy")
    return runner.invoke(app, args)


def test__skip_empty_writes_nothing(sample_gtfs_path, tmp_path, lazy):
    output = tmp_path / "filtered"

    result = run_filter(sample_gtfs_path, output, EMPTY_BOUNDS, lazy, "--skip-empty")

    assert result.exit_code == 0
    assert not output.exists()
    assert "filtered GTFS feed is empty; no output written" in result.output
    assert "Wrote output" not in result.output


def test__without_skip_empty_writes_output(sample_gtfs_path, tmp_path):
    output = tmp_path / "filtered"

    result = run_filter(sample_gtfs_path, output, EMPTY_BOUNDS, False)

    assert result.exit_code == 0
    assert output.exists()
    assert "Wrote output" in result.output


def test__skip_empty_keeps_non_empty_output(sample_gtfs_path, tmp_path, lazy):
    output = tmp_path / "filtered"

    result = run_filter(
        sample_gtfs_path, output, NON_EMPTY_BOUNDS, lazy, "--skip-empty"
    )

    assert result.exit_code == 0
    assert output.exists()
    assert "Wrote output" in result.output
    assert "no output written" not in result.output


@pytest.mark.parametrize("output_exists", [False, True])
def test__skip_empty_with_overwrite_fails_without_modifying_output(
    sample_gtfs_path, tmp_path, output_exists
):
    output = tmp_path / "filtered"
    original_content = b"existing output"
    if output_exists:
        output.write_bytes(original_content)

    result = run_filter(
        sample_gtfs_path,
        output,
        EMPTY_BOUNDS,
        False,
        "--skip-empty",
        "--overwrite",
    )

    assert result.exit_code == 1
    assert "Refusing to overwrite" in result.output
    assert "Wrote output" not in result.output
    assert "no output written" not in result.output
    if output_exists:
        assert output.read_bytes() == original_content
    else:
        assert not output.exists()


def test__filter_help_documents_skip_empty():
    result = runner.invoke(app, ["filter", "--help"])

    output = click.unstyle(result.output)
    assert result.exit_code == 0
    assert "--skip-empty" in output
    assert "service days" in output
