#!/usr/bin/env bash
set -euo pipefail

compose_file="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/docker-compose.yml"
bootstrap_server="kafka-1:9092"

create_topic() {
  local topic="$1"
  local partitions="$2"
  local retention_ms="$3"
  local cleanup_policy="$4"

  docker compose -f "$compose_file" exec -T kafka-1 \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$bootstrap_server" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 3 \
    --config "retention.ms=$retention_ms" \
    --config "cleanup.policy=$cleanup_policy" \
    --config min.insync.replicas=2
}

create_topic urbanpulse.bus_gps 12 86400000 delete
create_topic urbanpulse.traffic_signals 6 604800000 delete
create_topic urbanpulse.air_quality 3 7776000000 delete
create_topic urbanpulse.smart_meters 12 31536000000 delete
create_topic urbanpulse.route_schedule 3 -1 compact
create_topic urbanpulse.bus_gps_enriched 12 86400000 delete
create_topic urbanpulse.incidents 6 2592000000 delete
create_topic urbanpulse.ward_energy_summary 6 31536000000 delete
create_topic urbanpulse.health_advisories 3 7776000000 delete
create_topic urbanpulse.dlq 6 7776000000 delete

docker compose -f "$compose_file" exec -T kafka-1 \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$bootstrap_server" --list

