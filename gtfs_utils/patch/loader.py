import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from gtfs_utils.patch.errors import PatchLoadError
from gtfs_utils.patch.model import (
    ANCHORS,
    DEFAULT_DEMAND_WINDOW_S,
    DemandSpec,
    DirectionEntry,
    InsertStops,
    NewStop,
    Patch,
    Selector,
    Station,
)

YAML_SUFFIXES = (".yaml", ".yml")
JSON_SUFFIXES = (".json",)
YAML_INSTALL_HINT = "YAML patches need pyyaml: pip install 'gtfsutils[yaml]'"

# patch key -> stops.txt column
STOP_ATTRIBUTES = {
    "name": "stop_name",
    "stop_code": "stop_code",
    "platform_code": "platform_code",
    "wheelchair_boarding": "wheelchair_boarding",
}


def yaml_available() -> bool:
    try:
        import yaml  # noqa: F401
    except ImportError:
        return False
    return True


def load_patch(source: str | Path | Mapping[str, Any]) -> Patch:
    """
    Load and validate a patch.

    :param source: path to a `.yaml`, `.yml` or `.json` file, or the already parsed patch
    :return: the validated patch
    :raises PatchLoadError: if the file can't be read or the patch is invalid
    """
    if isinstance(source, Mapping):
        return _Parser("").patch(source)

    path = Path(source)
    suffix = path.suffix.lower()
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as e:
        raise PatchLoadError(str(path), f"can't read file: {e}", code="load_failed")

    if suffix in YAML_SUFFIXES:
        try:
            import yaml
        except ImportError:
            raise PatchLoadError(str(path), YAML_INSTALL_HINT, code="yaml_missing")
        try:
            data = yaml.safe_load(text)
        except yaml.YAMLError as e:
            raise PatchLoadError(str(path), f"invalid YAML: {e}", code="load_failed")
    elif suffix in JSON_SUFFIXES:
        try:
            data = json.loads(text)
        except json.JSONDecodeError as e:
            raise PatchLoadError(str(path), f"invalid JSON: {e}", code="load_failed")
    else:
        raise PatchLoadError(
            str(path),
            f"unknown patch format '{suffix}', use .yaml, .yml or .json",
            code="load_failed",
        )

    return _Parser(str(path)).patch(data)


class _Parser:
    def __init__(self, file: str) -> None:
        self.file = file

    def error(self, where: str, message: str) -> PatchLoadError:
        return PatchLoadError(self.file, f"{where}: {message}" if where else message)

    def mapping(
        self,
        value: Any,
        where: str,
        required: tuple[str, ...] = (),
        optional: tuple[str, ...] = (),
        any_keys: bool = False,
    ) -> Mapping[str, Any]:
        if not isinstance(value, Mapping):
            raise self.error(where, "must be a mapping")
        unknown = [k for k in value if k not in required + optional]
        if unknown and not any_keys:
            raise self.error(where, f"unknown key(s) {', '.join(map(str, unknown))}")
        missing = [k for k in required if k not in value]
        if missing:
            raise self.error(where, f"missing key(s) {', '.join(missing)}")
        return value

    def string(self, value: Any, where: str) -> str:
        if not isinstance(value, str) or not value:
            hint = " (quote it in YAML)" if isinstance(value, (int, float)) else ""
            raise self.error(where, f"must be a non-empty string{hint}")
        return value

    def strings(self, value: Any, where: str) -> tuple[str, ...]:
        if isinstance(value, list):
            if not value:
                raise self.error(where, "must not be empty")
            return tuple(self.string(v, f"{where}[{i}]") for i, v in enumerate(value))
        return (self.string(value, where),)

    def integer(self, value: Any, where: str, minimum: int | None = 0) -> int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise self.error(where, "must be an integer")
        if minimum is not None and value < minimum:
            raise self.error(where, f"must be >= {minimum}")
        return value

    def number(self, value: Any, where: str) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise self.error(where, "must be a number")
        return float(value)

    def direction_id(self, value: Any, where: str) -> int:
        if isinstance(value, str) and value.isdigit():
            value = int(value)
        if isinstance(value, bool) or value not in (0, 1):
            raise self.error(where, "direction_id must be 0 or 1")
        return value

    def coordinates(
        self, value: Mapping[str, Any], where: str
    ) -> tuple[float | None, float | None]:
        if ("lat" in value) != ("lon" in value):
            raise self.error(where, "lat and lon must be given together")
        if "lat" not in value:
            return None, None
        lat = self.number(value["lat"], f"{where}.lat")
        lon = self.number(value["lon"], f"{where}.lon")
        if not -90 <= lat <= 90 or not -180 <= lon <= 180:
            raise self.error(where, "lat/lon out of range")
        return lat, lon

    def attributes(self, value: Mapping[str, Any], where: str) -> dict[str, str]:
        attributes = {}
        for key, column in STOP_ATTRIBUTES.items():
            if key in value:
                raw = value[key]
                if key == "wheelchair_boarding":
                    raw = str(self.integer(raw, f"{where}.{key}"))
                attributes[column] = self.string(raw, f"{where}.{key}")
        return attributes

    def patch(self, data: Any) -> Patch:
        data = self.mapping(data, "", ("version", "ops"), ("base_feed",))
        if isinstance(data["version"], bool) or data["version"] != 1:
            raise self.error("version", "only version 1 is supported")
        base_feed = None
        if "base_feed" in data:
            base_feed = self.string(data["base_feed"], "base_feed")

        ops = data["ops"]
        if not isinstance(ops, list) or not ops:
            raise self.error("ops", "must be a non-empty list")
        return Patch(
            version=1,
            ops=tuple(self.op(op, f"ops[{i}]") for i, op in enumerate(ops)),
            base_feed=base_feed,
        )

    def op(self, value: Any, where: str) -> InsertStops:
        if isinstance(value, Mapping) and value.get("op") != "insert_stops":
            raise self.error(f"{where}.op", "only 'insert_stops' is supported")
        value = self.mapping(
            value,
            where,
            ("op", "select", "per_direction"),
            ("id", "station", "travel_time"),
        )

        if "travel_time" in value:
            travel_time = self.mapping(
                value["travel_time"], f"{where}.travel_time", ("mode",)
            )
            if travel_time["mode"] != "fixed":
                raise self.error(
                    f"{where}.travel_time.mode", "only 'fixed' is supported"
                )

        per_direction = self.mapping(
            value["per_direction"], f"{where}.per_direction", any_keys=True
        )
        if not per_direction:
            raise self.error(f"{where}.per_direction", "must not be empty")
        directions = []
        for key, entry in per_direction.items():
            entry_where = f"{where}.per_direction.{key}"
            directions.append(
                self.direction(self.direction_id(key, entry_where), entry, entry_where)
            )
        if len({d.direction_id for d in directions}) != len(directions):
            raise self.error(f"{where}.per_direction", "direction given twice")

        stop_ids = [d.stop.stop_id for d in directions]
        if len(set(stop_ids)) != len(stop_ids):
            raise self.error(
                f"{where}.per_direction", "each direction needs its own new stop_id"
            )

        station = None
        if "station" in value:
            station = self.station(value["station"], f"{where}.station")
            if station.stop_id in stop_ids:
                raise self.error(
                    f"{where}.station.stop_id",
                    "must differ from the new stops' stop_ids",
                )

        return InsertStops(
            select=self.selector(value["select"], f"{where}.select"),
            directions=tuple(directions),
            station=station,
            id=self.string(value["id"], f"{where}.id") if "id" in value else None,
        )

    def selector(self, value: Any, where: str) -> Selector:
        value = self.mapping(
            value,
            where,
            ("route_id",),
            ("direction_id", "service_id", "trip_id", "shift_id"),
        )
        direction_id = None
        if "direction_id" in value:
            raw = value["direction_id"]
            raw = raw if isinstance(raw, list) else [raw]
            direction_id = tuple(
                self.direction_id(d, f"{where}.direction_id") for d in raw
            )

        def optional(key: str) -> tuple[str, ...] | None:
            return self.strings(value[key], f"{where}.{key}") if key in value else None

        return Selector(
            route_id=self.strings(value["route_id"], f"{where}.route_id"),
            direction_id=direction_id,
            service_id=optional("service_id"),
            trip_id=optional("trip_id"),
            shift_id=optional("shift_id"),
        )

    def station(self, value: Any, where: str) -> Station:
        value = self.mapping(value, where, ("stop_id",), ("lat", "lon", "name"))
        lat, lon = self.coordinates(value, where)
        return Station(
            stop_id=self.string(value["stop_id"], f"{where}.stop_id"),
            lat=lat,
            lon=lon,
            attributes=self.attributes(value, where),
        )

    def new_stop(self, value: Any, where: str) -> NewStop:
        value = self.mapping(
            value, where, ("stop_id",), ("lat", "lon", *STOP_ATTRIBUTES)
        )
        lat, lon = self.coordinates(value, where)
        return NewStop(
            stop_id=self.string(value["stop_id"], f"{where}.stop_id"),
            lat=lat,
            lon=lon,
            attributes=self.attributes(value, where),
        )

    def direction(self, direction_id: int, value: Any, where: str) -> DirectionEntry:
        value = self.mapping(
            value,
            where,
            ("stop", "between"),
            ("anchor", "delta_s", "dwell_s", "offset_s", "demand"),
        )
        stop = self.new_stop(value["stop"], f"{where}.stop")

        between = value["between"]
        if not isinstance(between, list) or len(between) != 2:
            raise self.error(f"{where}.between", "must be a list of two stop_ids")
        a, b = (self.string(s, f"{where}.between[{i}]") for i, s in enumerate(between))
        if a == b:
            raise self.error(f"{where}.between", "stop_ids must differ")
        if stop.stop_id in (a, b):
            raise self.error(f"{where}.between", "must not contain the new stop")

        anchor = value.get("anchor")
        if anchor is not None and anchor not in ANCHORS:
            raise self.error(f"{where}.anchor", f"must be one of {', '.join(ANCHORS)}")

        delta_s = self.integer(value.get("delta_s", 0), f"{where}.delta_s")
        dwell_s = self.integer(value.get("dwell_s", 0), f"{where}.dwell_s")
        offset_s = None
        if "offset_s" in value:
            offset_s = self.integer(value["offset_s"], f"{where}.offset_s")

        if anchor == "untimed":
            timed = [
                k for k in ("delta_s", "dwell_s", "offset_s", "demand") if value.get(k)
            ]
            if timed:
                raise self.error(
                    where, f"anchor 'untimed' can't be combined with {', '.join(timed)}"
                )
        elif delta_s > 0 and anchor is None:
            raise self.error(
                f"{where}.anchor", "required when delta_s > 0 (first_stop or last_stop)"
            )

        demand = None
        if "demand" in value:
            demand = self.demand(value["demand"], f"{where}.demand")

        return DirectionEntry(
            direction_id=direction_id,
            stop=stop,
            between=(a, b),
            anchor=anchor,
            delta_s=delta_s,
            dwell_s=dwell_s,
            offset_s=offset_s,
            demand=demand,
        )

    def demand(self, value: Any, where: str) -> DemandSpec:
        value = self.mapping(value, where, ("value",), ("by_shift", "window_s", "note"))
        by_shift = {}
        if "by_shift" in value:
            raw = self.mapping(value["by_shift"], f"{where}.by_shift", any_keys=True)
            by_shift = {
                self.string(k, f"{where}.by_shift"): self.integer(
                    v, f"{where}.by_shift.{k}"
                )
                for k, v in raw.items()
            }

        window_s = DEFAULT_DEMAND_WINDOW_S
        if "window_s" in value:
            raw = value["window_s"]
            if not isinstance(raw, list) or len(raw) != 2:
                raise self.error(f"{where}.window_s", "must be [before, after]")
            before, after = (
                self.integer(v, f"{where}.window_s[{i}]", minimum=None)
                for i, v in enumerate(raw)
            )
            if before > after:
                raise self.error(f"{where}.window_s", "before must be <= after")
            window_s = (before, after)

        return DemandSpec(
            value=self.integer(value["value"], f"{where}.value"),
            by_shift=by_shift,
            window_s=window_s,
            note=self.string(value["note"], f"{where}.note")
            if "note" in value
            else None,
        )
