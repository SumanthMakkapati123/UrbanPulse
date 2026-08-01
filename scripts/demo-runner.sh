#!/usr/bin/env bash
# UrbanPulse Interactive Demo Runner
# Run from urbanpulse-submission:  ./scripts/demo-runner.sh
set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m'

# Track background PIDs for cleanup
BACKGROUND_PIDS=()

cleanup() {
  echo ""
  echo -e "${RED}${BOLD}Cleaning up …${NC}"
  if [ ${#BACKGROUND_PIDS[@]} -gt 0 ]; then
    for pid in "${BACKGROUND_PIDS[@]}"; do
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    done
  fi
  echo -e "${GREEN}Done.${NC}"
}
trap cleanup EXIT

banner() {
  echo ""
  echo -e "${CYAN}═══════════════════════════════════════════════════════════${NC}"
  echo -e "${BOLD}${GREEN}  $1${NC}"
  echo -e "${CYAN}═══════════════════════════════════════════════════════════${NC}"
  echo ""
}

# Print the command being run so the viewer can see it
cmd() {
  echo -e "  ${DIM}\$ $*${NC}"
}

step() {
  echo ""
  echo -e "  ${YELLOW}▶ $1${NC}"
}

pause() {
  echo ""
  echo -e "  ${BOLD}Press ENTER to continue …${NC}"
  read -r
}

# Run a python module with IPv6 noise filtered out (foreground)
py_run() {
  cmd "python -m $*"
  python -m "$@" 2> >(grep -v '%[0-9]|' >&2)
}

# Run a python module in the background silently
py_bg() {
  cmd "python -m $* &"
  python -m "$@" 2>/dev/null &
  BACKGROUND_PIDS+=($!)
  disown
}

kill_bg() {
  if [ ${#BACKGROUND_PIDS[@]} -gt 0 ]; then
    for pid in "${BACKGROUND_PIDS[@]}"; do
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    done
  fi
  BACKGROUND_PIDS=()
}

# Activate venv
source "$PROJECT_DIR/.venv/bin/activate"
export PYTHONPATH="$PROJECT_DIR/python"
export KAFKA_BOOTSTRAP_SERVERS="127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094"

########################################################################
banner "PART 1: ARCHITECTURE & KAFKA CLUSTER"
########################################################################

# Check if Kafka brokers are already running
RUNNING_BROKERS=$(docker compose -f infra/docker-compose.yml ps --status running 2>/dev/null | grep -c 'kafka' || true)

if [ "$RUNNING_BROKERS" -ge 3 ]; then
  echo -e "  ${GREEN}✔ Kafka cluster already running ($RUNNING_BROKERS brokers detected) — skipping startup${NC}"
else
  step "Starting 3-broker Kafka cluster …"
  cmd "docker compose -f infra/docker-compose.yml up -d kafka-1 kafka-2 kafka-3"
  docker compose -f infra/docker-compose.yml up -d kafka-1 kafka-2 kafka-3 2>&1 | tail -5

  step "Waiting for KRaft election …"
  sleep 20
fi

step "Cluster status …"
cmd "docker compose -f infra/docker-compose.yml ps"
docker compose -f infra/docker-compose.yml ps 2>&1

step "KRaft quorum status …"
cmd "kafka-metadata-quorum.sh --bootstrap-server kafka-1:9092 describe --status"
docker compose -f infra/docker-compose.yml exec -T kafka-1 \
  /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kafka-1:9092 describe --status 2>&1

step "Application topics (excluding internal topics) …"
cmd "kafka-topics.sh --bootstrap-server kafka-1:9092 --describe"
docker compose -f infra/docker-compose.yml exec -T kafka-1 \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 --describe 2>&1 | grep -v '__consumer_offsets\|__transaction'

pause

########################################################################
banner "PART 2: INGESTION, DLQ & ENRICHMENT"
########################################################################

step "Loading route schedule into Kafka …"
py_run urbanpulse.load_route_schedule reference-data/route_schedule.csv
echo -e "  ${GREEN}✔ Route schedule loaded${NC}"

step "Starting route enrichment engine (background) …"
py_bg urbanpulse.route_enrichment --schedule-csv reference-data/route_schedule.csv
sleep 3
echo -e "  ${GREEN}✔ Enrichment engine running${NC}"

step "Starting producers (background) …"
py_bg urbanpulse.bus_gps_producer --events 50000 --rate 200
py_bg urbanpulse.traffic_signal_producer --events 50000 --rate 200
py_bg urbanpulse.air_quality_producer --events 50000 --rate 60 --null-rate 0.05
py_bg urbanpulse.smart_meter_producer --events 50000 --rate 500
echo -e "  ${GREEN}✔ 4 producers streaming data continuously${NC}"

echo -e "  Letting producers generate data for 30 seconds …"
sleep 30

step "Tailing raw bus GPS events …"
py_run urbanpulse.tail_topic urbanpulse.bus_gps --messages 1 || true

step "Tailing raw traffic signal events …"
py_run urbanpulse.tail_topic urbanpulse.traffic_signals --messages 1 || true

step "Tailing raw air quality events …"
py_run urbanpulse.tail_topic urbanpulse.air_quality --messages 1 || true

step "Tailing raw smart meter events …"
py_run urbanpulse.tail_topic urbanpulse.smart_meters --messages 1 || true

step "Tailing enriched bus GPS events (with route schedule join) …"
py_run urbanpulse.tail_topic urbanpulse.bus_gps_enriched --messages 1 || true

step "Starting producers with high error rates for DLQ demo …"
py_bg urbanpulse.air_quality_producer --events 300 --rate 30 --null-rate 0.10 --out-of-range-rate 0.05
py_bg urbanpulse.bus_gps_producer --events 200 --rate 30 --invalid-gps-rate 0.10
sleep 5

step "Capturing DLQ report (15 seconds) …"
py_run urbanpulse.dlq_report --duration 15 --output evidence/dlq-report.csv || true
echo ""
echo -e "  ${CYAN}--- DLQ Report ---${NC}"
cat evidence/dlq-report.csv 2>/dev/null || echo "  (no DLQ events captured)"

step "Tailing DLQ topic (sample bad event envelope) …"
py_run urbanpulse.tail_topic urbanpulse.dlq --messages 1 || true

pause

########################################################################
banner "PART 3: PRIORITY CONSUMERS & PYFLINK"
########################################################################

step "Stopping producers …"
kill_bg
sleep 2

step "Running priority consumer lag demo …"
cmd "./scripts/run-priority-demo.sh"
./scripts/run-priority-demo.sh 2>&1 || true

echo ""
echo -e "  ${CYAN}--- Consumer Lag Report ---${NC}"
cat evidence/consumer-lag.csv 2>/dev/null || echo "  (lag report not generated)"

pause

step "Starting Flink & submitting incident job …"
cmd "./scripts/run-flink-incidents.sh"
./scripts/run-flink-incidents.sh >/dev/null 2>&1 &
BACKGROUND_PIDS+=($!)
disown

echo -e "  Waiting 15 seconds for Flink to start …"
sleep 15
echo ""
echo -e "  ${GREEN}${BOLD}>>> Open Flink UI: http://localhost:8081 <<<${NC}"
echo ""

step "Publishing incident test fixtures …"
py_run urbanpulse.incident_fixture_producer || true

step "Tailing incident alerts …"
py_run urbanpulse.tail_topic urbanpulse.incidents --messages 2 --timeout 90 || true

pause

########################################################################
banner "PART 4: PYSPARK ANALYTICS"
########################################################################

step "Stopping Flink …"
kill_bg
sleep 2

step "Starting Spark ward energy aggregation …"
cmd "./scripts/run-spark-ward-energy.sh"
./scripts/run-spark-ward-energy.sh >/dev/null 2>&1 &
BACKGROUND_PIDS+=($!)
disown

step "Starting Spark health advisories …"
cmd "./scripts/run-spark-health-advisories.sh"
./scripts/run-spark-health-advisories.sh >/dev/null 2>&1 &
BACKGROUND_PIDS+=($!)
disown

echo -e "  Waiting 15 seconds for Spark jobs …"
sleep 15

step "Starting producers to feed Spark streams …"
py_bg urbanpulse.air_quality_producer --events 50 --rate 10
py_bg urbanpulse.smart_meter_producer --events 200 --rate 100
echo -e "  ${GREEN}✔ Producers running${NC}"

step "Tailing ward energy summary …"
py_run urbanpulse.tail_topic urbanpulse.ward_energy_summary --messages 2 --timeout 90 || true

step "Tailing health advisories …"
py_run urbanpulse.tail_topic urbanpulse.health_advisories --messages 2 --timeout 90 || true

step "Showing Parquet output tree …"
cmd "find output/ward_energy -maxdepth 3 -type d"
find output/ward_energy -maxdepth 3 -type d 2>/dev/null | sort || echo "  (no Parquet output yet)"

pause

########################################################################
banner "DEMO COMPLETE — TEARING DOWN"
########################################################################

step "Stopping all background processes …"
kill_bg

step "Stopping Spark containers …"
docker stop urbanpulse-spark-ward 2>/dev/null || true
docker stop urbanpulse-spark-aqi 2>/dev/null || true

step "Stopping Docker compose services …"
cmd "docker compose -f infra/docker-compose.yml --profile processing down"
docker compose -f infra/docker-compose.yml --profile processing down 2>/dev/null || true

echo ""
echo -e "  ${GREEN}${BOLD}Demo finished successfully!${NC}"
echo ""

