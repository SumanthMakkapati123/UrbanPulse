#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
network_name="urbanpulse_default"

docker run --rm --name urbanpulse-spark-ward --network "$network_name" \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092,kafka-3:9092 \
  -v "$repo_dir/spark:/opt/urbanpulse/spark:ro" \
  -v "$repo_dir/output:/opt/urbanpulse/output" \
  -v "$repo_dir/checkpoints:/opt/urbanpulse/checkpoints" \
  spark:4.1.2-python3 \
  /opt/spark/bin/spark-submit \
  --master local[4] \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2 \
  /opt/urbanpulse/spark/ward_energy_stream.py
