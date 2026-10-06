from dask import is_dask_collection

from gtfs_utils.patch.errors import PatchError
from gtfs_utils.patch.integrity import check_foreign_keys
from gtfs_utils.patch.model import InsertStops, Patch
from gtfs_utils.patch.ops import insert_stops
from gtfs_utils.patch.report import PatchReport
from gtfs_utils.utils import DelayedGtfsDict, GtfsDict

PATCHED_FILES = ("trips", "stops", "stop_times")


def apply_patch(gtfs: GtfsDict, patch: Patch) -> tuple[GtfsDict, PatchReport]:
    """
    Apply a patch to a feed. The input feed is not modified.

    :param gtfs: eagerly loaded feed (`lazy=False`)
    :param patch: the patch, see `load_patch`
    :return: the patched feed and a report of patched and skipped trips
    :raises PatchError: if any op aborts, the input feed stays unchanged
    """
    for file in PATCHED_FILES:
        if is_dask_collection(gtfs[file]):
            raise PatchError("lazy_feed", "patches need an eagerly loaded feed")

    result = _copy(gtfs)
    report = PatchReport()
    for index, op in enumerate(patch.ops):
        if isinstance(op, InsertStops):
            report.ops.append(insert_stops(result, op, index))
        else:
            raise PatchError("unknown_op", f"ops[{index}]: {type(op).__name__}")

    check_foreign_keys(result, baseline=gtfs)
    return result, report


def _copy(gtfs: GtfsDict) -> GtfsDict:
    """Shallow copy: frames are shared until an op replaces them, unread files stay unread."""
    if isinstance(gtfs, DelayedGtfsDict):
        copy = DelayedGtfsDict(
            base_file=gtfs.base_file,
            existing_files=gtfs.existing_files,
            lazy=gtfs.lazy,
            specs=gtfs.specs,
        )
    else:
        copy = GtfsDict()
        copy.specs = gtfs.specs
    copy.store.update(gtfs.store)
    return copy
