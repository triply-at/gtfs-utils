from enum import Enum

from gtfs_utils.spec import FileSpec, ForeignKey, GtfsSpec


# https://github.com/triply-at/triply-specs/tree/main/specs/gtfs-demand-vehicles
class DemandVehiclesFile(str, Enum):
    DEMANDS = "demands"
    VEHICLES = "vehicles"
    SHIFTS = "shifts"

    @property
    def file(self) -> str:
        return self.value


GTFS_DEMAND_VEHICLES = GtfsSpec(
    name="gtfs-demand-vehicles",
    files=(
        FileSpec(
            DemandVehiclesFile.DEMANDS.file,
            foreign_keys=(
                ForeignKey("trip_id", "trips", "trip_id"),
                ForeignKey("stop_id", "stops", "stop_id"),
            ),
        ),
        FileSpec(
            DemandVehiclesFile.VEHICLES.file,
            foreign_keys=(ForeignKey("trip_id", "trips", "trip_id"),),
        ),
        # company-wide catalog, kept even if no remaining trip references a shift
        FileSpec(DemandVehiclesFile.SHIFTS.file),
    ),
    dtypes={
        # demands.txt
        "demand": "UInt32",
        "earliest_time": "string",
        "latest_time": "string",
        "demand_note": "string",
        # vehicles.txt
        "vehicle_id": "string",
        "capacity": "float64",
        "vehicle_name": "string",
        "provider": "string",
        "vehicle_category": "string",
        "expected_occupation": "float64",
        "average_occupation": "float64",
        "cost_fixed": "float64",
        "cost_per_km": "float64",
        "cost_per_hour": "float64",
        "currency": "string",
        # shifts.txt + trips.txt
        "shift_id": "string",
        "shift_name": "string",
        "shift_start": "string",
        "shift_end": "string",
        "shift_note": "string",
    },
)
