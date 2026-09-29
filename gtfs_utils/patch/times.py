import pandas as pd


def parse_time(value: str | None) -> int | None:
    """
    Parse a GTFS time (`H:MM:SS` or `HH:MM:SS`, hours may exceed 24) into seconds since service-day start.

    :param value: the time string, empty values (None, NA, "") are returned as None
    :return: seconds or None
    """
    if value is None or value is pd.NA or (isinstance(value, float) and pd.isna(value)):
        return None
    value = value.strip()
    if not value:
        return None

    parts = value.split(":")
    if len(parts) != 3 or not all(p.isdigit() for p in parts):
        raise ValueError(f"Invalid GTFS time: {value!r}")
    hours, minutes, seconds = map(int, parts)
    if minutes > 59 or seconds > 59:
        raise ValueError(f"Invalid GTFS time: {value!r}")
    return hours * 3600 + minutes * 60 + seconds


def format_time(seconds: int | None) -> str | None:
    """
    Format seconds since service-day start as `HH:MM:SS`, hours beyond 24 are kept.

    :param seconds: non-negative seconds or None
    :return: the time string or None
    """
    if seconds is None:
        return None
    if seconds < 0:
        raise ValueError(f"GTFS times can't be negative: {seconds}")
    hours, rest = divmod(seconds, 3600)
    minutes, secs = divmod(rest, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"
