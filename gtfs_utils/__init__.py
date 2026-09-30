# ruff: noqa: F401

import importlib.metadata

__version__ = importlib.metadata.version("gtfsutils")

from .utils import load_gtfs_delayed
from .utils import GtfsFile, DelayedGtfsDict, GTFS, DEFAULT_SPECS
from .spec import GtfsSpec, FileSpec, ForeignKey
from .extensions import GTFS_DEMAND_VEHICLES, DemandVehiclesFile

from .info import get_info, get_bounding_box, get_calendar_date_range, get_route_types
from .filter import filter_gtfs
from .patch import apply_patch, load_patch
