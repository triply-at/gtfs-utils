from collections import defaultdict
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from gtfs_utils.patch.errors import PatchError
from gtfs_utils.patch.geo import haversine_m
from gtfs_utils.patch.model import DirectionEntry, InsertStops, Selector
from gtfs_utils.patch.report import OpReport
from gtfs_utils.patch.times import format_time, parse_time
from gtfs_utils.utils import GtfsDict

STOP_REUSE_RADIUS_M = 25
COPIED_STOP_TIME_COLUMNS = (
    "stop_headsign",
    "pickup_type",
    "drop_off_type",
    "continuous_pickup",
    "continuous_drop_off",
    "timepoint",
)

Coordinates = tuple[float, float]


@dataclass
class _StopTimeChanges:
    times: dict[tuple[int, str], str] = field(default_factory=dict)
    """(row label, column) -> new time"""
    sequences: dict[int, int] = field(default_factory=dict)
    """row label -> new stop_sequence"""
    new_rows: list[tuple[int, dict]] = field(default_factory=list)
    """(row label of the stop the new row follows, row values)"""


@dataclass(frozen=True)
class _ResolvedStop:
    stop_id: str
    coordinates: Coordinates
    exists: bool


def insert_stops(gtfs: GtfsDict, op: InsertStops, op_index: int) -> OpReport:
    """
    Insert new stops into the selected trips of `gtfs`. Nothing is written unless all checks pass.

    :param gtfs: eager feed, its stops and stop_times are replaced
    :param op: the operation
    :param op_index: position of the op in the patch, used in the report
    :return: report of patched and skipped trips
    :raises PatchError: if the op can't be applied
    """
    report = OpReport(op.id or f"ops[{op_index}]")
    stops = gtfs.stops()
    stop_times = gtfs.stop_times()

    trips = _select_trips(gtfs, op.select)
    if trips.empty:
        raise PatchError(
            "empty_selection", f"{report.op_id}: selector matches no trips"
        )
    report.trips_selected = len(trips)

    entries = {entry.direction_id: entry for entry in op.directions}
    trips = trips[trips["direction_id"].isin(entries).fillna(False)]
    _check_frequencies(gtfs, trips["trip_id"])
    if any(entry.demand for entry in op.directions) and "demands" not in gtfs:
        raise PatchError(
            "extension_missing",
            "demand needs demands.txt from the gtfs-demand-vehicles extension",
        )

    coordinates = _stop_coordinates(stops)
    changes = _StopTimeChanges()
    used_stops: list[tuple[DirectionEntry, _ResolvedStop]] = []

    for entry in op.directions:
        new_stop = _resolve_stop(entry, coordinates)
        trip_ids = trips.loc[trips["direction_id"] == entry.direction_id, "trip_id"]
        patched = _plan_direction(
            stop_times, trip_ids, entry, new_stop, coordinates, changes, report
        )
        report.trips_patched[entry.direction_id] = patched
        if patched:
            used_stops.append((entry, new_stop))

    new_stop_rows = _new_stop_rows(stops, op, used_stops)
    report.stops_added = [row["stop_id"] for row in new_stop_rows]
    demand_rows = _demand_rows(gtfs, op, trips, changes)

    gtfs["stop_times"] = _apply_stop_time_changes(stop_times, changes)
    if new_stop_rows:
        gtfs["stops"] = _append_rows(stops, new_stop_rows)
    if demand_rows:
        gtfs["demands"] = _append_rows(gtfs.demands(), demand_rows)
    report.demands_added = len(demand_rows)
    return report


def _select_trips(gtfs: GtfsDict, select: Selector) -> pd.DataFrame:
    trips = gtfs.trips()
    mask = trips["route_id"].isin(select.route_id)
    for column in ("direction_id", "service_id", "trip_id", "shift_id"):
        values = getattr(select, column)
        if values is None:
            continue
        if column not in trips.columns:
            code = "extension_missing" if column == "shift_id" else "column_missing"
            raise PatchError(code, f"trips.txt has no column {column}")
        mask &= trips[column].isin(values).fillna(False)
    return trips[mask]


def _check_frequencies(gtfs: GtfsDict, trip_ids: pd.Series) -> None:
    if "frequencies" not in gtfs:
        return
    frequency_trips = set(gtfs.frequencies()["trip_id"]) & set(trip_ids)
    if frequency_trips:
        raise PatchError(
            "frequency_trip",
            f"frequency-based trips are not supported yet: {', '.join(sorted(frequency_trips))}",
        )


def _stop_coordinates(stops: pd.DataFrame) -> dict[str, Coordinates]:
    return {
        stop_id: (float(lat), float(lon))
        for stop_id, lat, lon in zip(
            stops["stop_id"], stops["stop_lat"], stops["stop_lon"]
        )
        if pd.notna(lat) and pd.notna(lon)
    }


def _resolve_stop(
    entry: DirectionEntry, coordinates: dict[str, Coordinates]
) -> _ResolvedStop:
    stop = entry.stop
    existing = coordinates.get(stop.stop_id)
    if stop.lat is None:
        if existing is None:
            raise PatchError(
                "stop_missing",
                f"stop {stop.stop_id} is not in the feed, lat and lon are required",
            )
        return _ResolvedStop(stop.stop_id, existing, exists=True)

    if existing is not None:
        distance = haversine_m(stop.lat, stop.lon, *existing)
        if distance > STOP_REUSE_RADIUS_M:
            raise PatchError(
                "stop_conflict",
                f"stop {stop.stop_id} already exists {distance:.0f} m away from the patch coordinates",
            )
        return _ResolvedStop(stop.stop_id, existing, exists=True)
    return _ResolvedStop(stop.stop_id, (stop.lat, stop.lon), exists=False)


def _plan_direction(
    stop_times: pd.DataFrame,
    trip_ids: pd.Series,
    entry: DirectionEntry,
    new_stop: _ResolvedStop,
    coordinates: dict[str, Coordinates],
    changes: _StopTimeChanges,
    report: OpReport,
) -> int:
    rows = stop_times[stop_times["trip_id"].isin(trip_ids)].sort_values(
        ["trip_id", "stop_sequence"], kind="stable"
    )
    if rows.empty:
        return 0

    trips_by_pattern: dict[tuple, list[str]] = defaultdict(list)
    for trip_id, pattern in (
        rows.groupby("trip_id", sort=False)["stop_id"].agg(list).items()
    ):
        trips_by_pattern[tuple(pattern)].append(trip_id)

    position_by_trip: dict[str, int] = {}
    a, b = entry.between
    for pattern, pattern_trips in trips_by_pattern.items():
        if new_stop.stop_id in pattern:
            report.skip(pattern_trips, "already_present")
            continue
        matches = [
            i
            for i in range(len(pattern) - 1)
            if pattern[i] == a and pattern[i + 1] == b
        ]
        if not matches:
            report.skip(pattern_trips, "not_adjacent", f"{a} -> {b}")
        elif len(matches) > 1:
            report.skip(pattern_trips, "ambiguous_position", f"{a} -> {b}")
        else:
            position_by_trip.update(dict.fromkeys(pattern_trips, matches[0]))

    share = _distance_share(
        coordinates.get(a), new_stop.coordinates, coordinates.get(b)
    )
    patched = 0
    for trip_id, trip_rows in rows.groupby("trip_id", sort=False):
        position = position_by_trip.get(trip_id)
        if position is None:
            continue
        if _plan_trip(
            trip_id, trip_rows, position, entry, new_stop, share, changes, report
        ):
            patched += 1
    return patched


def _distance_share(
    a: Coordinates | None, x: Coordinates, b: Coordinates | None
) -> float:
    """Share of the A -> B running time spent on A -> X, by straight-line distance."""
    if a is None or b is None:
        return 0.5
    ax, xb = haversine_m(*a, *x), haversine_m(*x, *b)
    return ax / (ax + xb) if ax + xb > 0 else 0.5


def _plan_trip(
    trip_id: str,
    rows: pd.DataFrame,
    position: int,
    entry: DirectionEntry,
    new_stop: _ResolvedStop,
    share: float,
    changes: _StopTimeChanges,
    report: OpReport,
) -> bool:
    labels = rows.index.to_list()
    arrivals = [parse_time(t) for t in rows["arrival_time"]]
    departures = [parse_time(t) for t in rows["departure_time"]]
    sequences = [int(s) for s in rows["stop_sequence"]]
    a, b = position, position + 1

    new_arrival = new_departure = None
    shifted: dict[int, int] = {}
    if entry.anchor != "untimed":
        a_departure = departures[a] if departures[a] is not None else arrivals[a]
        b_arrival = arrivals[b] if arrivals[b] is not None else departures[b]
        if a_departure is None or b_arrival is None:
            report.skip([trip_id], "neighbour_untimed")
            return False

        running = b_arrival - a_departure + entry.delta_s - entry.dwell_s
        offset = (
            entry.offset_s if entry.offset_s is not None else round(running * share)
        )
        if running < 0 or offset > running:
            report.skip(
                [trip_id],
                "offset_too_large",
                f"{running}s between {entry.between[0]} and {entry.between[1]}",
            )
            return False

        if entry.anchor == "last_stop":
            shifted = {i: -entry.delta_s for i in range(b)}
            a_departure -= entry.delta_s
        elif entry.delta_s:
            shifted = {i: entry.delta_s for i in range(b, len(labels))}
        new_arrival = a_departure + offset
        new_departure = new_arrival + entry.dwell_s

    for i, delta in shifted.items():
        for column, values in (
            ("arrival_time", arrivals),
            ("departure_time", departures),
        ):
            if values[i] is None:
                continue
            if values[i] + delta < 0:
                raise PatchError(
                    "negative_time",
                    f"trip {trip_id} would reach stop {rows['stop_id'].iloc[i]} before 00:00:00",
                )
            changes.times[(labels[i], column)] = format_time(values[i] + delta)

    if sequences[b] - sequences[a] > 1:
        new_sequence = sequences[a] + 1
    else:
        new_sequence = sequences[b]
        for i in range(b, len(labels)):
            changes.sequences[labels[i]] = sequences[i] + 1

    a_row = rows.iloc[a]
    new_row = {
        column: a_row[column]
        for column in COPIED_STOP_TIME_COLUMNS
        if column in rows.columns
    }
    new_row.update(
        trip_id=trip_id,
        stop_id=new_stop.stop_id,
        arrival_time=format_time(new_arrival),
        departure_time=format_time(new_departure),
        stop_sequence=new_sequence,
    )
    if entry.anchor == "untimed":
        new_row["timepoint"] = 0
    changes.new_rows.append((labels[a], new_row))
    return True


def _is_station(stop: pd.Series) -> bool:
    location_type = stop.get("location_type")
    return pd.notna(location_type) and int(location_type) == 1


def _new_stop_rows(
    stops: pd.DataFrame,
    op: InsertStops,
    used_stops: list[tuple[DirectionEntry, _ResolvedStop]],
) -> list[dict]:
    rows = []
    station = op.station
    if station is not None and used_stops:
        existing = stops[stops["stop_id"] == station.stop_id]
        if existing.empty:
            if station.lat is not None:
                lat, lon = station.lat, station.lon
            else:
                lat = float(np.mean([s.coordinates[0] for _, s in used_stops]))
                lon = float(np.mean([s.coordinates[1] for _, s in used_stops]))
            rows.append(
                {
                    "stop_id": station.stop_id,
                    **station.attributes,
                    "stop_lat": lat,
                    "stop_lon": lon,
                    "location_type": 1,
                }
            )
        elif not _is_station(existing.iloc[0]):
            raise PatchError(
                "stop_conflict",
                f"station {station.stop_id} already exists and is not a station (location_type=1)",
            )

    for entry, new_stop in used_stops:
        if new_stop.exists:
            continue
        row = {
            "stop_id": new_stop.stop_id,
            **entry.stop.attributes,
            "stop_lat": new_stop.coordinates[0],
            "stop_lon": new_stop.coordinates[1],
        }
        if "location_type" in stops.columns:
            row["location_type"] = 0
        if "zone_id" in stops.columns:
            # fare zone of the preceding stop
            previous = stops.loc[stops["stop_id"] == entry.between[0], "zone_id"]
            if not previous.empty and pd.notna(previous.iloc[0]):
                row["zone_id"] = previous.iloc[0]
        if station is not None:
            row["parent_station"] = station.stop_id
        rows.append(row)
    return rows


def _demand_rows(
    gtfs: GtfsDict, op: InsertStops, trips: pd.DataFrame, changes: _StopTimeChanges
) -> list[dict]:
    demand_by_stop = {e.stop.stop_id: e.demand for e in op.directions if e.demand}
    if not demand_by_stop:
        return []

    shift_by_trip = {}
    if "shift_id" in trips.columns:
        shift_by_trip = dict(zip(trips["trip_id"], trips["shift_id"]))
    demands = gtfs.demands()
    existing = set(zip(demands["trip_id"], demands["stop_id"]))

    rows = []
    for _, stop_time in changes.new_rows:
        demand = demand_by_stop.get(stop_time["stop_id"])
        if demand is None:
            continue
        trip_id, stop_id = stop_time["trip_id"], stop_time["stop_id"]
        if (trip_id, stop_id) in existing:
            raise PatchError(
                "demand_exists",
                f"demands.txt already has a row for trip {trip_id} at stop {stop_id}",
            )
        shift_id = shift_by_trip.get(trip_id)
        departure = parse_time(stop_time["departure_time"])
        before, after = demand.window_s
        row = {
            "trip_id": trip_id,
            "stop_id": stop_id,
            "demand": demand.for_shift(None if pd.isna(shift_id) else shift_id),
            "earliest_time": format_time(max(0, departure + before)),
            "latest_time": format_time(max(0, departure + after)),
        }
        if demand.note is not None:
            row["demand_note"] = demand.note
        rows.append(row)
    return rows


def _append_rows(df: pd.DataFrame, rows: list[dict]) -> pd.DataFrame:
    new = pd.DataFrame(rows)
    new = new.astype({c: df[c].dtype for c in new.columns if c in df.columns})
    return pd.concat([df, new], ignore_index=True)


def _apply_stop_time_changes(
    stop_times: pd.DataFrame, changes: _StopTimeChanges
) -> pd.DataFrame:
    if not changes.new_rows:
        return stop_times

    result = stop_times.copy()
    by_column: dict[str, dict[int, str]] = defaultdict(dict)
    for (label, column), value in changes.times.items():
        by_column[column][label] = value
    for column, values in by_column.items():
        result.loc[list(values), column] = list(values.values())
    if changes.sequences:
        result.loc[list(changes.sequences), "stop_sequence"] = list(
            changes.sequences.values()
        )

    after_labels = [label for label, _ in changes.new_rows]
    new = pd.DataFrame([row for _, row in changes.new_rows])
    new = new.astype({c: result[c].dtype for c in new.columns if c in result.columns})

    order = np.concatenate(
        [
            np.arange(len(result), dtype=float),
            result.index.get_indexer(after_labels) + 0.5,
        ]
    )
    combined = pd.concat([result, new], ignore_index=True)
    return combined.iloc[np.argsort(order, kind="stable")].reset_index(drop=True)
