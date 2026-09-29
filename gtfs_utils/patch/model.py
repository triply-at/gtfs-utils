from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal

Anchor = Literal["first_stop", "last_stop", "untimed"]
ANCHORS: tuple[str, ...] = ("first_stop", "last_stop", "untimed")

DEFAULT_DEMAND_WINDOW_S: tuple[int, int] = (-600, 300)


@dataclass(frozen=True)
class Selector:
    """Selects trips, all given keys must match. `None` means no restriction."""

    route_id: tuple[str, ...]
    direction_id: tuple[int, ...] | None = None
    service_id: tuple[str, ...] | None = None
    trip_id: tuple[str, ...] | None = None
    shift_id: tuple[str, ...] | None = None


@dataclass(frozen=True)
class NewStop:
    stop_id: str
    lat: float | None = None
    lon: float | None = None
    attributes: Mapping[str, str] = field(default_factory=dict)
    """Additional stops.txt columns, e.g. stop_name"""


@dataclass(frozen=True)
class Station:
    stop_id: str
    lat: float | None = None
    lon: float | None = None
    attributes: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class DemandSpec:
    value: int
    by_shift: Mapping[str, int] = field(default_factory=dict)
    window_s: tuple[int, int] = DEFAULT_DEMAND_WINDOW_S
    note: str | None = None

    def for_shift(self, shift_id: str | None) -> int:
        if shift_id is None:
            return self.value
        return self.by_shift.get(shift_id, self.value)


@dataclass(frozen=True)
class DirectionEntry:
    direction_id: int
    stop: NewStop
    between: tuple[str, str]
    anchor: Anchor | None = None
    delta_s: int = 0
    dwell_s: int = 0
    offset_s: int | None = None
    """Seconds from A's departure to the new stop's arrival, `None` splits by distance"""
    demand: DemandSpec | None = None


@dataclass(frozen=True)
class InsertStops:
    select: Selector
    directions: tuple[DirectionEntry, ...]
    station: Station | None = None
    id: str | None = None


Operation = InsertStops


@dataclass(frozen=True)
class Patch:
    version: int
    ops: tuple[Operation, ...]
    base_feed: str | None = None
