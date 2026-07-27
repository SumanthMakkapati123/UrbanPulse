from __future__ import annotations

import argparse
import csv
import json
import logging
import time
from pathlib import Path

from confluent_kafka import Consumer

from .config import BOOTSTRAP_SERVERS, TOPICS
from .enrichment import enrich_bus_event
from .kafka_io import ReliableJsonProducer


def load_csv(path: Path) -> dict[str, dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return {row["route_id"]: row for row in csv.DictReader(handle)}


def make_consumer(group_id: str, topic: str, offset: str) -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": group_id,
            "auto.offset.reset": offset,
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    return consumer


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Python materialized-table equivalent of the route_schedule KTable join"
    )
    parser.add_argument("--schedule-csv", type=Path, required=True)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    route_table = load_csv(args.schedule_csv)
    schedule_consumer = make_consumer(
        "urbanpulse-python-route-table-v1", TOPICS["route_schedule"], "earliest"
    )
    bus_consumer = make_consumer(
        "urbanpulse-python-route-enrichment-v1", TOPICS["bus_gps"], "earliest"
    )
    producer = ReliableJsonProducer("urbanpulse-python-route-enrichment")
    pending = []

    try:
        while True:
            # Drain schedule changes first. The compacted topic behaves as the
            # changelog for this in-memory materialized table.
            while True:
                update = schedule_consumer.poll(0)
                if update is None:
                    break
                if not update.error() and update.key() and update.value():
                    route_id = update.key().decode("utf-8")
                    route_table[route_id] = json.loads(update.value())

            message = bus_consumer.poll(1.0)
            if message is None or message.error():
                continue
            bus = json.loads(message.value())
            route_id = message.key().decode("utf-8") if message.key() else bus.get("route_id")
            enriched = enrich_bus_event(bus, route_table.get(route_id))
            producer.produce("urbanpulse.bus_gps_enriched", route_id, enriched)
            pending.append(message)

            if len(pending) >= 100:
                producer.flush()
                bus_consumer.commit(asynchronous=False)
                pending.clear()
    except KeyboardInterrupt:
        logging.info("Stopping route enrichment")
    finally:
        if pending:
            producer.flush()
            bus_consumer.commit(asynchronous=False)
        schedule_consumer.close()
        bus_consumer.close()


if __name__ == "__main__":
    main()
