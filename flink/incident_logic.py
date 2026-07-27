from __future__ import annotations

import math


def pair_key(first_bus_id: str, second_bus_id: str) -> str:
    return "|".join(sorted((first_bus_id, second_bus_id)))


def haversine_metres(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    earth_radius_metres = 6_371_000.0
    lat_delta = math.radians(lat2 - lat1)
    lon_delta = math.radians(lon2 - lon1)
    a = (
        math.sin(lat_delta / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(lon_delta / 2) ** 2
    )
    return earth_radius_metres * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

