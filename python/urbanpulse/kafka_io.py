from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Iterable

from confluent_kafka import KafkaException, Producer

from .config import TOPICS, reliable_producer_config
from .validation import ValidationError


LOGGER = logging.getLogger(__name__)


def json_bytes(value: dict[str, Any]) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")


class ReliableJsonProducer:
    def __init__(self, client_id: str) -> None:
        self._producer = Producer(reliable_producer_config(client_id))

    @staticmethod
    def _delivery_callback(error: object, message: object) -> None:
        if error is not None:
            LOGGER.error("Kafka delivery failed: %s", error)

    def produce(self, topic: str, key: str, event: dict[str, Any]) -> None:
        while True:
            try:
                self._producer.produce(
                    topic=topic,
                    key=key.encode("utf-8"),
                    value=json_bytes(event),
                    on_delivery=self._delivery_callback,
                )
                self._producer.poll(0)
                return
            except BufferError:
                LOGGER.warning("Producer queue full; applying back-pressure")
                self._producer.poll(1.0)
            except KafkaException:
                LOGGER.exception("Kafka produce call failed")
                raise

    def route_or_dlq(
        self,
        source_topic: str,
        key: str,
        event: dict[str, Any],
        errors: Iterable[ValidationError],
    ) -> bool:
        failures = list(errors)
        if not failures:
            self.produce(source_topic, key, event)
            return True

        for failure in failures:
            envelope = {
                "original_topic": source_topic,
                "original_key": key,
                "error_type": failure.error_type,
                "error_reason": failure.reason,
                "failed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "event": event,
            }
            self.produce(TOPICS["dlq"], key, envelope)
            LOGGER.warning(
                "Routed invalid event to DLQ: type=%s key=%s reason=%s",
                failure.error_type,
                key,
                failure.reason,
            )
        return False

    def flush(self, timeout: float = 30.0) -> None:
        outstanding = self._producer.flush(timeout)
        if outstanding:
            raise RuntimeError(f"{outstanding} Kafka message(s) were not delivered before timeout")

