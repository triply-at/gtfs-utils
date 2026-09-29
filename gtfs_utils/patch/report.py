from dataclasses import dataclass, field


@dataclass(frozen=True)
class SkippedTrip:
    trip_id: str
    code: str
    detail: str = ""


@dataclass
class OpReport:
    op_id: str
    trips_selected: int = 0
    trips_patched: dict[int, int] = field(default_factory=dict)
    """Patched trips per direction_id"""
    skipped: list[SkippedTrip] = field(default_factory=list)
    stops_added: list[str] = field(default_factory=list)

    def skip(self, trip_ids, code: str, detail: str = "") -> None:
        self.skipped.extend(SkippedTrip(t, code, detail) for t in trip_ids)
