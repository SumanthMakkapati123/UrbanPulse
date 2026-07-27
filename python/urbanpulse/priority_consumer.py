from __future__ import annotations

import argparse
import json
import logging
import time

from confluent_kafka import Consumer, KafkaError

from .config import BOOTSTRAP_SERVERS, TOPICS


GROUPS = {
    "high": "traffic-signals-high-priority",
    "standard": "traffic-signals-standard-priority",
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one priority-group Kafka consumer")
    parser.add_argument("--priority", choices=GROUPS, required=True)
    parser.add_argument("--consumer-id", required=True)
    parser.add_argument("--delay-ms", type=int, default=None)
    args = parser.parse_args()

    default_delay = 0 if args.priority == "high" else 250
    delay_ms = default_delay if args.delay_ms is None else args.delay_ms
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUPS[args.priority],
            "client.id": args.consumer_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": 300_000,
        }
    )
    consumer.subscribe([TOPICS["traffic_signals"]])
    processed = 0
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(message.error())
            json.loads(message.value())
            if delay_ms:
                time.sleep(delay_ms / 1000.0)
            processed += 1
            if processed % 100 == 0:
                consumer.commit(asynchronous=True)
            if processed % 100 == 0:
                logging.info(
                    "priority=%s consumer=%s processed=%d partition=%d offset=%d",
                    args.priority,
                    args.consumer_id,
                    processed,
                    message.partition(),
                    message.offset(),
                )
    except KeyboardInterrupt:
        logging.info("Stopping consumer after %d records", processed)
    finally:
        if processed:
            consumer.commit(asynchronous=False)
        consumer.close()


if __name__ == "__main__":
    main()
