#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"
export PYTHONPATH="$repo_dir/python${PYTHONPATH:+:$PYTHONPATH}"

mkdir -p evidence/raw
pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT INT TERM

python -m urbanpulse.priority_consumer --priority high --consumer-id high-1 \
  >evidence/raw/high-priority.log 2>&1 & pids+=("$!")
for consumer_id in 1 2 3; do
  python -m urbanpulse.priority_consumer --priority standard \
    --consumer-id "standard-$consumer_id" --delay-ms 250 \
    >"evidence/raw/standard-$consumer_id.log" 2>&1 & pids+=("$!")
done

python -m urbanpulse.traffic_signal_producer --events 30000 --rate 380 \
  >evidence/raw/traffic-producer.log 2>&1 & pids+=("$!")

"$repo_dir/scripts/capture-consumer-lag.sh" 120
