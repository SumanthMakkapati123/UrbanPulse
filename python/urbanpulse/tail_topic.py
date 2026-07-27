from __future__ import annotations

import argparse
import json
import time

from confluent_kafka import Consumer

from .config import BOOTSTRAP_SERVERS


def main() -> None:
    parser = argparse.ArgumentParser(description="Print a bounded sample from any UrbanPulse topic")
    parser.add_argument("topic")
    parser.add_argument("--messages", type=int, default=10)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": f"urbanpulse-inspector-{int(time.time() * 1000)}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([args.topic])
    deadline = time.monotonic() + args.timeout
    count = 0
    try:
        while count < args.messages and time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None or message.error():
                continue
            value = json.loads(message.value())
            print(json.dumps(value, indent=2, sort_keys=True))
            count += 1
    finally:
        consumer.close()
    print(f"Displayed {count} message(s) from {args.topic}")


if __name__ == "__main__":
    main()

