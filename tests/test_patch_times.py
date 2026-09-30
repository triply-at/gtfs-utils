import pandas as pd
import pytest

from gtfs_utils.patch.times import format_time, parse_time


@pytest.mark.parametrize(
    "value, expected",
    [
        ("6:00:00", 21600),
        ("06:00:00", 21600),
        ("08:43:20", 31400),
        ("30:05:00", 108300),
        ("00:00:00", 0),
        (" 07:35:00 ", 27300),
    ],
)
def test_parse_time(value, expected):
    assert parse_time(value) == expected


@pytest.mark.parametrize("value", [None, "", "  ", pd.NA, float("nan")])
def test_parse_empty_time(value):
    assert parse_time(value) is None


@pytest.mark.parametrize(
    "value", ["6:00", "a:00:00", "06:60:00", "06:00:60", "-1:00:00"]
)
def test_parse_invalid_time(value):
    with pytest.raises(ValueError):
        parse_time(value)


@pytest.mark.parametrize(
    "seconds, expected",
    [(21600, "06:00:00"), (31400, "08:43:20"), (108300, "30:05:00"), (0, "00:00:00")],
)
def test_format_time(seconds, expected):
    assert format_time(seconds) == expected


def test_format_empty_time():
    assert format_time(None) is None


def test_format_negative_time():
    with pytest.raises(ValueError):
        format_time(-1)


def test_roundtrip():
    for value in ["05:30:00", "23:59:59", "24:00:00", "47:12:03"]:
        assert format_time(parse_time(value)) == value
