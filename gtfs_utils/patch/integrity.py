import pandas as pd

from gtfs_utils.patch.errors import PatchError
from gtfs_utils.spec import ForeignKey
from gtfs_utils.utils import GtfsDict

# references a patch can break in the base files, extension references come from gtfs.specs
PATCHED_REFERENCES: tuple[tuple[str, ForeignKey], ...] = (
    ("stop_times", ForeignKey("trip_id", "trips", "trip_id")),
    ("stop_times", ForeignKey("stop_id", "stops", "stop_id")),
    ("stops", ForeignKey("parent_station", "stops", "stop_id")),
)

Reference = tuple[str, ForeignKey]


def unresolved_references(gtfs: GtfsDict) -> dict[Reference, set]:
    """Values of each reference that don't exist in the referenced file."""
    references = list(PATCHED_REFERENCES) + [
        (spec.file, fk) for spec in gtfs.file_specs() for fk in spec.foreign_keys
    ]
    result = {}
    for file, fk in references:
        if file not in gtfs or fk.ref_file not in gtfs:
            continue
        df = gtfs[file]
        if fk.column not in df.columns:
            continue
        values = set(pd.Series(df[fk.column]).dropna())
        missing = values - set(gtfs[fk.ref_file][fk.ref_column])
        if missing:
            result[(file, fk)] = missing
    return result


def check_foreign_keys(gtfs: GtfsDict, baseline: GtfsDict | None = None) -> None:
    """
    Assert that the patch left no reference unresolved.

    :param gtfs: the patched feed
    :param baseline: the unpatched feed, references already broken there are ignored
    :raises PatchError: with code `foreign_key` listing the first unresolved values
    """
    before = unresolved_references(baseline) if baseline is not None else {}
    for (file, fk), missing in unresolved_references(gtfs).items():
        missing -= before.get((file, fk), set())
        if missing:
            sample = ", ".join(sorted(map(str, missing))[:5])
            raise PatchError(
                "foreign_key",
                f"{file}.{fk.column} references missing {fk.ref_file}.{fk.ref_column}: {sample}",
            )
