from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Iterator

from incident_logic import haversine_metres, pair_key
from pyflink.common import Duration, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor


BUNCHING_DISTANCE_METRES = 200.0
BUNCHING_DURATION_MS = 5 * 60 * 1000
MAX_POSITION_AGE_MS = 90 * 1000


def epoch_millis(timestamp: str) -> int:
    parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    return int(parsed.timestamp() * 1000)


def utc_iso(epoch_ms: int) -> str:
    return datetime.fromtimestamp(epoch_ms / 1000, timezone.utc).isoformat().replace("+00:00", "Z")


def alert_json(
    incident_type: str,
    entity_id: str,
    event_time_ms: int,
    zone: str | None,
    details: dict[str, Any],
) -> str:
    detected_at_ms = int(time.time() * 1000)
    return json.dumps(
        {
            "incident_type": incident_type,
            "entity_id": entity_id,
            "zone": zone,
            "event_time": utc_iso(event_time_ms),
            "detected_at": utc_iso(detected_at_ms),
            "detection_latency_ms": max(0, detected_at_ms - event_time_ms),
            "details": details,
        },
        separators=(",", ":"),
        sort_keys=True,
    )


class JsonTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value: str, record_timestamp: int) -> int:
        return epoch_millis(json.loads(value)["timestamp"])


class AqiEmergencyDetector(KeyedProcessFunction):
    def open(self, runtime_context) -> None:
        self.last_alerted_reading = runtime_context.get_state(
            ValueStateDescriptor("last-alerted-aqi-reading", Types.LONG())
        )

    def process_element(self, value: str, ctx) -> Iterator[str]:
        event = json.loads(value)
        event_time = epoch_millis(event["timestamp"])
        previous = self.last_alerted_reading.value()
        if event["aqi"] > 300 and (previous is None or event_time > previous):
            self.last_alerted_reading.update(event_time)
            yield alert_json(
                "AQI_EMERGENCY",
                event["sensor_id"],
                event_time,
                event["zone"],
                {"aqi": event["aqi"], "threshold": 300},
            )


class GridlockDetector(KeyedProcessFunction):
    def open(self, runtime_context) -> None:
        self.consecutive_breaches = runtime_context.get_state(
            ValueStateDescriptor("consecutive-gridlock-cycles", Types.INT())
        )

    def process_element(self, value: str, ctx) -> Iterator[str]:
        event = json.loads(value)
        current = self.consecutive_breaches.value() or 0
        if event["avg_wait_sec"] > 180:
            current += 1
            self.consecutive_breaches.update(current)
            if current == 3:
                yield alert_json(
                    "TRAFFIC_GRIDLOCK",
                    event["junction_id"],
                    epoch_millis(event["timestamp"]),
                    event["zone"],
                    {
                        "avg_wait_sec": event["avg_wait_sec"],
                        "consecutive_cycles": current,
                    },
                )
        else:
            self.consecutive_breaches.clear()


class BusBunchingDetector(KeyedProcessFunction):
    def open(self, runtime_context) -> None:
        self.latest_positions = runtime_context.get_map_state(
            MapStateDescriptor("latest-bus-positions", Types.STRING(), Types.STRING())
        )
        self.pair_start_times = runtime_context.get_map_state(
            MapStateDescriptor("bunching-pair-start-times", Types.STRING(), Types.LONG())
        )
        self.pair_timers = runtime_context.get_map_state(
            MapStateDescriptor("bunching-pair-timers", Types.STRING(), Types.LONG())
        )
        self.alerted_pairs = runtime_context.get_map_state(
            MapStateDescriptor("already-alerted-pairs", Types.STRING(), Types.BOOLEAN())
        )

    def process_element(self, value: str, ctx) -> Iterator[str]:
        current = json.loads(value)
        current["event_time_ms"] = epoch_millis(current["timestamp"])
        self.latest_positions.put(current["bus_id"], json.dumps(current))

        for other_bus_id, other_json in self.latest_positions.items():
            if other_bus_id == current["bus_id"]:
                continue
            other = json.loads(other_json)
            pair = pair_key(current["bus_id"], other_bus_id)
            distance = haversine_metres(
                current["lat"], current["lon"], other["lat"], other["lon"]
            )
            if distance <= BUNCHING_DISTANCE_METRES:
                if not self.alerted_pairs.get(pair) and not self.pair_start_times.contains(pair):
                    started_at = max(current["event_time_ms"], other["event_time_ms"])
                    timer_at = started_at + BUNCHING_DURATION_MS
                    self.pair_start_times.put(pair, started_at)
                    self.pair_timers.put(pair, timer_at)
                    ctx.timer_service().register_event_time_timer(timer_at)
            else:
                timer_at = self.pair_timers.get(pair)
                if timer_at is not None:
                    ctx.timer_service().delete_event_time_timer(timer_at)
                self.pair_start_times.remove(pair)
                self.pair_timers.remove(pair)
                self.alerted_pairs.remove(pair)
        # This operator emits only from event-time timers. Keeping the method a
        # generator is required by the PyFlink ProcessFunction contract.
        if False:
            yield ""

    def on_timer(self, timestamp: int, ctx) -> Iterator[str]:
        for pair, timer_at in list(self.pair_timers.items()):
            if timer_at != timestamp:
                continue
            first_id, second_id = pair.split("|", maxsplit=1)
            first_json = self.latest_positions.get(first_id)
            second_json = self.latest_positions.get(second_id)
            started_at = self.pair_start_times.get(pair)
            if first_json and second_json and started_at is not None:
                first, second = json.loads(first_json), json.loads(second_json)
                distance = haversine_metres(
                    first["lat"], first["lon"], second["lat"], second["lon"]
                )
                latest_reading = min(first["event_time_ms"], second["event_time_ms"])
                positions_are_fresh = timestamp - latest_reading <= MAX_POSITION_AGE_MS
                if (
                    timestamp - started_at >= BUNCHING_DURATION_MS
                    and distance <= BUNCHING_DISTANCE_METRES
                    and positions_are_fresh
                ):
                    yield alert_json(
                        "BUS_BUNCHING",
                        ctx.get_current_key(),
                        timestamp,
                        None,
                        {
                            "bus_id_1": first_id,
                            "bus_id_2": second_id,
                            "route_id": ctx.get_current_key(),
                            "distance_metres": round(distance, 1),
                            "duration_seconds": BUNCHING_DURATION_MS // 1000,
                        },
                    )
                    self.alerted_pairs.put(pair, True)
            self.pair_start_times.remove(pair)
            self.pair_timers.remove(pair)


def kafka_source(topic: str, group_id: str, brokers: str) -> KafkaSource:
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(brokers)
        .set_topics(topic)
        .set_group_id(group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def watermarks() -> WatermarkStrategy:
    return (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(30))
        .with_timestamp_assigner(JsonTimestampAssigner())
        .with_idleness(Duration.of_minutes(1))
    )


def main() -> None:
    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
    env = StreamExecutionEnvironment.get_execution_environment()
    # Ship the pure-Python helper to Beam worker processes; a bind mount alone
    # is not added to the worker harness import path.
    env.add_python_file(os.path.join(os.path.dirname(__file__), "incident_logic.py"))
    env.enable_checkpointing(30_000, CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(10_000)
    env.set_parallelism(int(os.getenv("URBANPULSE_PARALLELISM", "4")))

    aqi = env.from_source(
        kafka_source("urbanpulse.air_quality", "pyflink-aqi-incidents", brokers),
        watermarks(),
        "Kafka AQI source",
    )
    traffic = env.from_source(
        kafka_source("urbanpulse.traffic_signals", "pyflink-gridlock-incidents", brokers),
        watermarks(),
        "Kafka traffic source",
    )
    buses = env.from_source(
        kafka_source("urbanpulse.bus_gps", "pyflink-bunching-incidents", brokers),
        watermarks(),
        "Kafka bus source",
    )

    aqi_alerts = aqi.key_by(lambda raw: json.loads(raw)["sensor_id"], Types.STRING()).process(
        AqiEmergencyDetector(), Types.STRING()
    )
    gridlock_alerts = traffic.key_by(
        lambda raw: json.loads(raw)["junction_id"], Types.STRING()
    ).process(GridlockDetector(), Types.STRING())
    bunching_alerts = buses.key_by(
        lambda raw: json.loads(raw)["route_id"], Types.STRING()
    ).process(BusBunchingDetector(), Types.STRING())

    incident_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(brokers)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("urbanpulse.incidents")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )
    aqi_alerts.union(gridlock_alerts, bunching_alerts).sink_to(incident_sink)
    env.execute("UrbanPulse PyFlink real-time incident detection")


if __name__ == "__main__":
    main()
