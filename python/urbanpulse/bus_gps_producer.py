from __future__ import annotations

import argparse
import logging
import random
import time
from datetime import datetime, timezone

from .config import TOPICS
from .kafka_io import ReliableJsonProducer
from .validation import validate_bus_gps


ROUTES = ("R101", "R102", "R103", "R104")


def generate_event(sequence: int, invalid_gps_rate: float = 0.01) -> dict[str, object]:
    route_id = ROUTES[sequence % len(ROUTES)]
    latitude = 91.0 if random.random() < invalid_gps_rate else round(
        12.9716 + random.uniform(-0.08, 0.08), 6
    )
    return {
        "bus_id": f"BUS-{sequence % 12000:05d}",
        "route_id": route_id,
        "lat": latitude,
        "lon": round(77.5946 + random.uniform(-0.08, 0.08), 6),
        "speed_kmh": round(random.uniform(0, 55), 1),
        "occupancy_pct": round(random.uniform(5, 100), 1),
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Produce simulated UrbanPulse bus GPS events")
    parser.add_argument("--events", type=int, default=1000)
    parser.add_argument("--rate", type=float, default=100.0, help="events per second; 0 means unlimited")
    parser.add_argument("--invalid-gps-rate", type=float, default=0.01)
    args = parser.parse_args()
    if not 0 <= args.invalid_gps_rate <= 1:
        parser.error("--invalid-gps-rate must be between 0 and 1")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    producer = ReliableJsonProducer("urbanpulse-bus-gps-producer")
    delay = 1.0 / args.rate if args.rate > 0 else 0.0

    for sequence in range(args.events):
        event = generate_event(sequence, args.invalid_gps_rate)
        route_id = str(event["route_id"])
        # route_id is the Kafka key, preserving per-route order within one partition.
        producer.route_or_dlq(TOPICS["bus_gps"], route_id, event, validate_bus_gps(event))
        if delay:
            time.sleep(delay)
    producer.flush()


if __name__ == "__main__":
    main()
