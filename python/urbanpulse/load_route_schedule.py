from __future__ import annotations

import argparse
import csv
from pathlib import Path

from .config import TOPICS
from .kafka_io import ReliableJsonProducer


def main() -> None:
    parser = argparse.ArgumentParser(description="Load route_schedule CSV into a compacted KTable topic")
    parser.add_argument("csv_path", type=Path)
    args = parser.parse_args()

    producer = ReliableJsonProducer("urbanpulse-route-schedule-loader")
    with args.csv_path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            route_id = row["route_id"]
            producer.produce(TOPICS["route_schedule"], route_id, row)
    producer.flush()


if __name__ == "__main__":
    main()

