from __future__ import annotations

import argparse
import random
import time
from datetime import datetime, timedelta, timezone

from .config import TOPICS
from .kafka_io import ReliableJsonProducer


def main() -> None:
    parser = argparse.ArgumentParser(description="Produce simulated smart-meter interval readings")
    parser.add_argument("--events", type=int, default=10000)
    parser.add_argument("--rate", type=float, default=1100.0)
    parser.add_argument(
        "--event-time-offset-minutes",
        type=float,
        default=0.0,
        help="test-only event-time offset; use a future sentinel to advance watermarks",
    )
    args = parser.parse_args()

    producer = ReliableJsonProducer("urbanpulse-smart-meter-producer")
    delay = 1.0 / args.rate if args.rate > 0 else 0.0
    for sequence in range(args.events):
        meter_id = f"M-{sequence % 1_100_000:07d}"
        ward_id = f"WARD-{sequence % 48 + 1:02d}"
        event = {
            "meter_id": meter_id,
            "ward_id": ward_id,
            # Simulation contract: kwh_reading is interval consumption, not a cumulative counter.
            "kwh_reading": round(random.uniform(0.05, 2.5), 4),
            "voltage": round(random.gauss(230, 8), 2),
            "power_factor": round(random.uniform(0.75, 1.0), 3),
            "timestamp": (
                datetime.now(timezone.utc)
                + timedelta(minutes=args.event_time_offset_minutes)
            ).isoformat().replace("+00:00", "Z"),
        }
        producer.produce(TOPICS["smart_meters"], meter_id, event)
        if delay:
            time.sleep(delay)
    producer.flush()


if __name__ == "__main__":
    main()
