# ruff: noqa: F401

import importlib.metadata

__version__ = importlib.metadata.version("gtfsutils")

from .extensions import GTFS_DEMAND_VEHICLES, DemandVehiclesFile
from .filter import filter_gtfs
from .info import get_bounding_box, get_calendar_date_range, get_info, get_route_types
from .patch import apply_patch, load_patch
from .spec import FileSpec, ForeignKey, GtfsSpec
from .utils import DEFAULT_SPECS, GTFS, DelayedGtfsDict, GtfsFile, load_gtfs_delayed
