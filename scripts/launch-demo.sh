#!/usr/bin/env bash
# macOS Terminal Demo Launcher for UrbanPulse

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "=================================================="
echo "Launching 4 Terminal windows for the UrbanPulse Demo..."
echo "=================================================="

# Function to launch a new Terminal window with environment set up
launch_window() {
  local title="$1"
  local init_command="$2"
  
  osascript -e "tell application \"Terminal\"
    activate
    set newWin to (do script \"cd '$PROJECT_DIR' && source .venv/bin/activate && export PYTHONPATH=\\\"\$PWD/python\\\" && export KAFKA_BOOTSTRAP_SERVERS=\\\"127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094\\\" && clear && echo '=== $title ===' && $init_command\")
  end tell"
}

# Window 1: Cluster control & Priority lag demo
launch_window "WINDOW 1: CLUSTER CONTROL & LAG DEMO" \
  "echo 'Commands ready for this window:'; echo '  1. Press ENTER to start Kafka Cluster'; echo '  2. Run Priority Demo: ./scripts/run-priority-demo.sh'; echo; read; docker compose -f infra/docker-compose.yml up -d kafka-1 kafka-2 kafka-3 && echo 'Waiting 15 seconds for Kafka to initialize...' && sleep 15 && ./infra/verify-cluster.sh"

# Window 2: Route schedules & enrichment
launch_window "WINDOW 2: ENRICHMENT & SCHEDULES" \
  "echo 'Commands ready for this window:'; echo '  Press ENTER to load schedules and start Enrichment'; echo; read; python -m urbanpulse.load_route_schedule reference-data/route_schedule.csv && python -m urbanpulse.route_enrichment --schedule-csv reference-data/route_schedule.csv"

# Window 3: Ingestion & Engine Jobs (Flink/Spark)
launch_window "WINDOW 3: PRODUCERS, FLINK & SPARK" \
  "echo 'Commands ready to run in this window:'; echo '  1. Start Ingestion: python -m urbanpulse.bus_gps_producer --events 20000 --rate 400 & python -m urbanpulse.air_quality_producer --events 5000 --rate 60 --null-rate 0.05 & python -m urbanpulse.smart_meter_producer --events 50000 --rate 1100 &'; echo '  2. Stop Ingestion: kill %1 %2 %3'; echo '  3. Flink Job: ./scripts/run-flink-incidents.sh'; echo '  4. Spark Energy: ./scripts/run-spark-ward-energy.sh'; echo '  5. Spark Advisories: ./scripts/run-spark-health-advisories.sh'; echo; bash"

# Window 4: Monitoring, DLQs, & Tailing
launch_window "WINDOW 4: MONITORING & AUDIT" \
  "echo 'Commands ready to run in this window:'; echo '  1. Tail Enriched: python -m urbanpulse.tail_topic urbanpulse.bus_gps_enriched --messages 3'; echo '  2. Capture DLQ: python -m urbanpulse.dlq_report --duration 10 --output evidence/dlq-report.csv && cat evidence/dlq-report.csv'; echo '  3. Inject Incident Fixture: python -m urbanpulse.incident_fixture_producer'; echo '  4. Tail Flink Alerts: python -m urbanpulse.tail_topic urbanpulse.incidents --messages 3 --timeout 90'; echo '  5. Tail Spark Ward output: python -m urbanpulse.tail_topic urbanpulse.ward_energy_summary --messages 3 --timeout 90'; echo '  6. Tail Spark Advisories: python -m urbanpulse.tail_topic urbanpulse.health_advisories --messages 3 --timeout 90'; echo; bash"
