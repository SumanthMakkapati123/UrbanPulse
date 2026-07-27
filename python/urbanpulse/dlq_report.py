from __future__ import annotations

import argparse
import csv
import json
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer

from .config import BOOTSTRAP_SERVERS, TOPICS


def main() -> None:
    parser = argparse.ArgumentParser(description="Produce a time-bounded DLQ error distribution")
    parser.add_argument("--duration", type=int, default=300, help="collection seconds; default is 5 min")
    parser.add_argument("--output", type=Path, default=Path("evidence/dlq-report.csv"))
    args = parser.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": f"dlq-report-{int(time.time())}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPICS["dlq"]])
    counts: Counter[str] = Counter()
    started_at = datetime.now(timezone.utc)
    deadline = time.monotonic() + args.duration
    try:
        while time.monotonic() < deadline:
            message = consumer.poll(min(1.0, max(0.0, deadline - time.monotonic())))
            if message is None or message.error():
                continue
            payload = json.loads(message.value())
            counts[payload.get("error_type", "UNKNOWN")] += 1
    finally:
        consumer.close()

    args.output.parent.mkdir(parents=True, exist_ok=True)
    total = sum(counts.values())
    with args.output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["window_started_utc", started_at.isoformat()])
        writer.writerow(["duration_seconds", args.duration])
        writer.writerow([])
        writer.writerow(["error_type", "count", "percentage"])
        for error_type, count in counts.most_common():
            writer.writerow([error_type, count, round(100.0 * count / total, 2) if total else 0.0])
        writer.writerow(["TOTAL", total, 100.0 if total else 0.0])
    print(f"Wrote {args.output} with {total} DLQ record(s)")


if __name__ == "__main__":
    main()

