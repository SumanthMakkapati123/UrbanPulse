from __future__ import annotations

import argparse
import logging
import random
import time
from datetime import datetime, timezone

from .config import TOPICS
from .kafka_io import ReliableJsonProducer
from .validation import validate_air_quality


ZONES = ("CENTRAL", "NORTH", "SOUTH", "EAST", "WEST")


def generate_event(
    sequence: int, null_rate: float, out_of_range_rate: float = 0.01
) -> dict[str, object]:
    failure_draw = random.random()
    if failure_draw < null_rate:
        aqi: int | None = None
    elif failure_draw < null_rate + out_of_range_rate:
        aqi = 550
    else:
        aqi = random.randint(35, 360)
    return {
        "sensor_id": f"AQ-{sequence % 600:04d}",
        "zone": ZONES[sequence % len(ZONES)],
        "pm25": round(random.uniform(10, 260), 1),
        "pm10": round(random.uniform(20, 340), 1),
        "no2": round(random.uniform(5, 140), 1),
        "aqi": aqi,
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Produce reliable UrbanPulse air-quality events")
    parser.add_argument("--events", type=int, default=1000)
    parser.add_argument("--rate", type=float, default=60.0)
    parser.add_argument("--null-rate", type=float, default=0.05)
    parser.add_argument("--out-of-range-rate", type=float, default=0.01)
    args = parser.parse_args()
    if not 0 <= args.null_rate <= 1 or not 0 <= args.out_of_range_rate <= 1:
        parser.error("failure rates must be between 0 and 1")
    if args.null_rate + args.out_of_range_rate > 1:
        parser.error("combined failure rates cannot exceed 1")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    producer = ReliableJsonProducer("urbanpulse-air-quality-producer")
    delay = 1.0 / args.rate if args.rate > 0 else 0.0

    for sequence in range(args.events):
        event = generate_event(sequence, args.null_rate, args.out_of_range_rate)
        sensor_id = str(event["sensor_id"])
        producer.route_or_dlq(
            TOPICS["air_quality"], sensor_id, event, validate_air_quality(event)
        )
        if delay:
            time.sleep(delay)
    producer.flush()


if __name__ == "__main__":
    main()
