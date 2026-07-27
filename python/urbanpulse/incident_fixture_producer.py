from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

from .config import TOPICS
from .kafka_io import ReliableJsonProducer


def iso(timestamp: datetime) -> str:
    return timestamp.isoformat().replace("+00:00", "Z")


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish deterministic events for all three Flink alerts")
    parser.add_argument(
        "--run-id",
        default="DEMO",
        help="fixture namespace so repeated demonstrations use fresh keyed state",
    )
    args = parser.parse_args()
    producer = ReliableJsonProducer("urbanpulse-incident-fixtures")
    now = datetime.now(timezone.utc)
    sensor_id = f"AQ-{args.run_id}-01"
    junction_id = f"J-{args.run_id}-01"
    first_bus = f"BUS-{args.run_id}-01"
    second_bus = f"BUS-{args.run_id}-02"

    producer.produce(
        TOPICS["air_quality"],
        sensor_id,
        {
            "sensor_id": sensor_id,
            "zone": "CENTRAL",
            "pm25": 260.0,
            "pm10": 330.0,
            "no2": 120.0,
            "aqi": 325,
            "timestamp": iso(now),
        },
    )

    for cycle, wait in enumerate((190.0, 205.0, 215.0), start=1):
        producer.produce(
            TOPICS["traffic_signals"],
            junction_id,
            {
                "junction_id": junction_id,
                "zone": "CENTRAL",
                "vehicle_count": 175,
                "avg_wait_sec": wait,
                "signal_phase": "NS_GREEN",
                "cycle_number": cycle,
                "timestamp": iso(now + timedelta(seconds=cycle)),
            },
        )

    # Thirteen chronological readings span six event-time minutes. The two
    # positions stay about 56 m apart, satisfying the 200 m / five-minute rule.
    start = now - timedelta(minutes=6)
    for step in range(13):
        event_time = start + timedelta(seconds=30 * step)
        for bus_id, latitude in ((first_bus, 12.971600), (second_bus, 12.972100)):
            producer.produce(
                TOPICS["bus_gps"],
                "R101",
                {
                    "bus_id": bus_id,
                    "route_id": "R101",
                    "lat": latitude,
                    "lon": 77.594600,
                    "speed_kmh": 18.0,
                    "occupancy_pct": 65.0,
                    "timestamp": iso(event_time),
                },
            )

    # A same-route sentinel guarantees that the source-partition watermark
    # advances beyond the five-minute timer even in a bounded demonstration.
    producer.produce(
        TOPICS["bus_gps"],
        "R101",
        {
            "bus_id": f"BUS-{args.run_id}-WATERMARK",
            "route_id": "R101",
            "lat": 13.071600,
            "lon": 77.694600,
            "speed_kmh": 0.0,
            "occupancy_pct": 0.0,
            "timestamp": iso(now + timedelta(minutes=10)),
        },
    )
    producer.flush()
    print("Published deterministic AQI, gridlock, and bus-bunching fixtures")


if __name__ == "__main__":
    main()
