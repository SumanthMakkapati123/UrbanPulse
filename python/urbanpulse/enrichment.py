from __future__ import annotations

from typing import Any, Mapping


def enrich_bus_event(
    bus_event: Mapping[str, Any], schedule: Mapping[str, Any] | None
) -> dict[str, Any]:
    """Left-join one bus record with the materialized route table."""
    enriched = dict(bus_event)
    if schedule is None:
        enriched["schedule_match"] = False
        enriched["scheduled_arrival_time"] = None
        enriched["route_name"] = None
        enriched["terminal"] = None
        return enriched

    enriched.update(
        {
            "scheduled_arrival_time": schedule["scheduled_arrival_time"],
            "route_name": schedule["route_name"],
            "terminal": schedule["terminal"],
            "schedule_match": True,
        }
    )
    return enriched

