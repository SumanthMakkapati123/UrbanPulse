#!/usr/bin/env bash
set -euo pipefail

compose_file="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/docker-compose.yml"

docker compose -f "$compose_file" ps
docker compose -f "$compose_file" exec -T kafka-1 \
  /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kafka-1:9092 describe --status
docker compose -f "$compose_file" exec -T kafka-1 \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 --describe

