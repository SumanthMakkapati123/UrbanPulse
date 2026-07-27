#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"

python_runtime="${PYTHON_RUNTIME:-python3}"
PYTHONPATH=python "$python_runtime" -m unittest discover -s python/tests -v
PYTHONPATH=flink "$python_runtime" -m unittest discover -s flink/tests -v
"$python_runtime" -m compileall -q python flink spark
"$python_runtime" scripts/verify_platform.py
docker compose -f infra/docker-compose.yml config --quiet
echo "Static verification passed"
