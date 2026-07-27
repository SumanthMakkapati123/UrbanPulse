from __future__ import annotations

import os


BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:19092,localhost:19093,localhost:19094",
)

TOPICS = {
    "bus_gps": "urbanpulse.bus_gps",
    "traffic_signals": "urbanpulse.traffic_signals",
    "air_quality": "urbanpulse.air_quality",
    "smart_meters": "urbanpulse.smart_meters",
    "route_schedule": "urbanpulse.route_schedule",
    "dlq": "urbanpulse.dlq",
}


def reliable_producer_config(client_id: str) -> dict[str, object]:
    """At-least-once delivery with Kafka idempotence and bounded in-flight ordering."""
    return {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": client_id,
        "acks": "all",
        "enable.idempotence": True,
        "retries": 2_147_483_647,
        "retry.backoff.ms": 250,
        "delivery.timeout.ms": 120_000,
        "request.timeout.ms": 30_000,
        "max.in.flight.requests.per.connection": 5,
        "compression.type": "lz4",
    }

