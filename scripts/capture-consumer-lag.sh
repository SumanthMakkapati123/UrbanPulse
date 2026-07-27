#!/usr/bin/env bash
set -euo pipefail

duration_seconds="${1:-120}"
repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$repo_dir/infra/docker-compose.yml"
output="$repo_dir/evidence/consumer-lag.csv"

printf 'timestamp_utc,group,total_lag\n' >"$output"
end_epoch=$((SECONDS + duration_seconds))
while (( SECONDS < end_epoch )); do
  timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  for group in traffic-signals-high-priority traffic-signals-standard-priority; do
    total_lag="$(docker compose -f "$compose_file" exec -T kafka-1 \
      /opt/kafka/bin/kafka-consumer-groups.sh \
      --bootstrap-server kafka-1:9092 --describe --group "$group" 2>/dev/null \
      | awk 'NR > 1 && $6 ~ /^[0-9]+$/ {sum += $6} END {print sum + 0}')"
    printf '%s,%s,%s\n' "$timestamp" "$group" "$total_lag" >>"$output"
  done
  sleep 5
done

echo "Wrote $output"
