from __future__ import annotations

import argparse
import random
import time
from datetime import datetime, timezone

from .config import TOPICS
from .kafka_io import ReliableJsonProducer


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate load for the priority-consumer demo")
    parser.add_argument("--events", type=int, default=30000)
    parser.add_argument("--rate", type=float, default=380.0)
    args = parser.parse_args()

    producer = ReliableJsonProducer("urbanpulse-traffic-signal-producer")
    delay = 1.0 / args.rate if args.rate > 0 else 0.0
    zones = ("CENTRAL", "NORTH", "SOUTH", "EAST", "WEST")
    phases = ("NS_GREEN", "EW_GREEN", "PEDESTRIAN")

    for sequence in range(args.events):
        junction_id = f"J-{sequence % 3800:04d}"
        event = {
            "junction_id": junction_id,
            "zone": zones[sequence % len(zones)],
            "vehicle_count": random.randint(0, 180),
            "avg_wait_sec": round(random.uniform(5, 240), 1),
            "signal_phase": phases[sequence % len(phases)],
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
        producer.produce(TOPICS["traffic_signals"], junction_id, event)
        if delay:
            time.sleep(delay)
    producer.flush()


if __name__ == "__main__":
    main()

