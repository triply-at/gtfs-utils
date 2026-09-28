from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field


@dataclass(frozen=True)
class ForeignKey:
    column: str
    ref_file: str
    ref_column: str


@dataclass(frozen=True)
class FileSpec:
    file: str
    required: bool = False
    foreign_keys: tuple[ForeignKey, ...] = ()
    """Rows whose keys no longer resolve are removed as orphans after filtering"""


@dataclass(frozen=True)
class GtfsSpec:
    """
    A set of GTFS files and column dtypes, either the base GTFS reference or an
    extension adding files / columns on top of it.
    """

    name: str
    files: tuple[FileSpec, ...] = ()
    dtypes: Mapping[str, str] = field(default_factory=dict)


def resolve_dtypes(specs: Iterable[GtfsSpec]) -> dict[str, str]:
    dtypes: dict[str, str] = {}
    for spec in specs:
        dtypes.update(spec.dtypes)
    return dtypes


def resolve_files(specs: Iterable[GtfsSpec]) -> list[FileSpec]:
    return [f for spec in specs for f in spec.files]
