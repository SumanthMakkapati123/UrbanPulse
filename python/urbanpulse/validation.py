from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping


@dataclass(frozen=True)
class ValidationError:
    error_type: str
    reason: str


def _missing(event: Mapping[str, Any], required: tuple[str, ...]) -> list[str]:
    return [field for field in required if field not in event]


def _valid_timestamp(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


def validate_air_quality(event: Mapping[str, Any]) -> list[ValidationError]:
    required = ("sensor_id", "zone", "pm25", "pm10", "no2", "aqi", "timestamp")
    missing = _missing(event, required)
    if missing:
        return [ValidationError("MISSING_FIELD", f"Missing fields: {', '.join(missing)}")]

    errors: list[ValidationError] = []
    if event["aqi"] is None:
        errors.append(ValidationError("NULL_AQI", "AQI value is null"))
    elif not isinstance(event["aqi"], (int, float)) or not 0 <= event["aqi"] <= 500:
        errors.append(ValidationError("AQI_OUT_OF_RANGE", "AQI must be between 0 and 500"))

    for pollutant in ("pm25", "pm10", "no2"):
        value = event[pollutant]
        if not isinstance(value, (int, float)) or value < 0:
            errors.append(
                ValidationError("POLLUTANT_OUT_OF_RANGE", f"{pollutant} must be non-negative")
            )

    if not _valid_timestamp(event["timestamp"]):
        errors.append(ValidationError("INVALID_TIMESTAMP", "timestamp must be ISO-8601"))
    return errors


def validate_bus_gps(event: Mapping[str, Any]) -> list[ValidationError]:
    required = (
        "bus_id",
        "route_id",
        "lat",
        "lon",
        "speed_kmh",
        "occupancy_pct",
        "timestamp",
    )
    missing = _missing(event, required)
    if missing:
        return [ValidationError("MISSING_FIELD", f"Missing fields: {', '.join(missing)}")]

    errors: list[ValidationError] = []
    lat, lon = event["lat"], event["lon"]
    if not isinstance(lat, (int, float)) or not -90 <= lat <= 90:
        errors.append(ValidationError("IMPOSSIBLE_GPS", "Latitude must be between -90 and 90"))
    if not isinstance(lon, (int, float)) or not -180 <= lon <= 180:
        errors.append(ValidationError("IMPOSSIBLE_GPS", "Longitude must be between -180 and 180"))
    if not isinstance(event["speed_kmh"], (int, float)) or not 0 <= event["speed_kmh"] <= 160:
        errors.append(ValidationError("SPEED_OUT_OF_RANGE", "speed_kmh must be between 0 and 160"))
    if not isinstance(event["occupancy_pct"], (int, float)) or not 0 <= event["occupancy_pct"] <= 100:
        errors.append(
            ValidationError("OCCUPANCY_OUT_OF_RANGE", "occupancy_pct must be between 0 and 100")
        )
    if not _valid_timestamp(event["timestamp"]):
        errors.append(ValidationError("INVALID_TIMESTAMP", "timestamp must be ISO-8601"))
    return errors

