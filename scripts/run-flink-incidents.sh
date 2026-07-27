#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$repo_dir/infra/docker-compose.yml"

docker compose -f "$compose_file" --profile processing up -d --build \
  kafka-1 kafka-2 kafka-3 flink-jobmanager flink-taskmanager
docker compose -f "$compose_file" exec -T flink-jobmanager \
  /opt/flink/bin/flink run -d --python /opt/flink/jobs/incident_detection.py
