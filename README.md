# UrbanPulse submission repository

Python-first implementation for the DSE ZG556 Situated Learning Assignment. It includes Kafka ingestion, validation/DLQ, priority consumers, route-table enrichment, PyFlink incidents, PySpark analytics, a consolidated report, and evidence scripts.

The project supports macOS Terminal and Windows PowerShell 7. Use [PLATFORM_SETUP.md](PLATFORM_SETUP.md) for native setup, command equivalents, Docker Desktop configuration, and troubleshooting.

## Important grading note

Apache Kafka Streams is a JVM library. `python/urbanpulse/route_enrichment.py` implements its required KTable behaviour in Python using a compacted changelog topic and an in-memory materialised table. It is behaviourally equivalent but is not the Java Kafka Streams API. If the assessor requires that exact library, the route-enrichment component cannot remain Python-only.

## Prerequisites

- Windows 10/11 with PowerShell 7 and Docker Desktop/WSL 2 Linux containers, or macOS 13+ with Docker Desktop
- Docker Desktop with at least 12 GB memory and 4 CPUs available
- Python 3.11 or 3.12
- 15-20 GB free disk for container images, Kafka data, checkpoints, and Spark packages
- Internet access on the first run to pull pinned images and Maven packages

## 1. Python environment

macOS:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r python/requirements.txt
export PYTHONPATH="$PWD/python"
```

Windows PowerShell 7:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r python\requirements.txt
$env:PYTHONPATH = (Join-Path $PWD "python")
```

Run local checks:

```text
# macOS
./scripts/verify-static.sh

# Windows PowerShell 7
.\scripts\verify-static.ps1
```

If host package installation is unavailable, build the equivalent isolated
runtime and set Kafka bootstrap addresses for the target network:

```bash
docker build -t urbanpulse/python:3.12 -f python/Dockerfile .
docker run --rm --network urbanpulse_default \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092,kafka-3:9092 \
  urbanpulse/python:3.12 urbanpulse.bus_gps_producer --events 100
```

## 2. Kafka cluster and topics

macOS:

```bash
docker compose -f infra/docker-compose.yml up -d kafka-1 kafka-2 kafka-3
./infra/create-topics.sh
./infra/verify-cluster.sh | tee evidence/cluster-verification.txt
```

Windows PowerShell 7:

```powershell
docker compose -f infra\docker-compose.yml up -d kafka-1 kafka-2 kafka-3
.\infra\create-topics.ps1
.\infra\verify-cluster.ps1 2>&1 | Tee-Object -FilePath evidence\cluster-verification.txt
```

Load the route reference table:

```bash
python -m urbanpulse.load_route_schedule reference-data/route_schedule.csv
```

Replace that development CSV with the official eLearn file before recording the final video.

## 3. Producers, DLQ, and route enrichment

Run each long-lived command in a separate terminal with the virtual environment active:

```bash
python -m urbanpulse.route_enrichment --schedule-csv reference-data/route_schedule.csv
python -m urbanpulse.bus_gps_producer --events 20000 --rate 400
python -m urbanpulse.air_quality_producer --events 5000 --rate 60 --null-rate 0.05
python -m urbanpulse.smart_meter_producer --events 50000 --rate 1100
```

Capture a five-minute DLQ distribution while invalid events are being generated:

```bash
python -m urbanpulse.dlq_report --duration 300 --output evidence/dlq-report.csv
```

Inspect the enriched result:

```bash
python -m urbanpulse.tail_topic urbanpulse.bus_gps_enriched --messages 3
```

## 4. Priority consumer demonstration

```text
# macOS
./scripts/run-priority-demo.sh

# Windows PowerShell 7
.\scripts\run-priority-demo.ps1
```

The script writes `evidence/consumer-lag.csv`. Plot or screenshot the two group-lag series for the report/video. The standard group should rise under its artificial delay; the high-priority group should remain close to zero after warm-up.

## 5. PyFlink incident detection

Build/start the processing profile and submit the job:

```text
# macOS
./scripts/run-flink-incidents.sh

# Windows PowerShell 7
.\scripts\run-flink-incidents.ps1
```

In another terminal, publish deterministic fixtures and inspect alerts:

```bash
python -m urbanpulse.incident_fixture_producer
python -m urbanpulse.tail_topic urbanpulse.incidents --messages 3 --timeout 90
```

Flink UI: http://localhost:8081

## 6. PySpark Structured Streaming

Run the energy and AQI jobs in separate terminals after the Kafka Compose network exists:

```text
# macOS — run in separate terminals
./scripts/run-spark-ward-energy.sh
./scripts/run-spark-health-advisories.sh

# Windows PowerShell 7 — run in separate terminals
.\scripts\run-spark-ward-energy.ps1
.\scripts\run-spark-health-advisories.ps1
```

Generate meter/AQI input, then inspect the outputs:

```bash
python -m urbanpulse.tail_topic urbanpulse.ward_energy_summary --messages 3 --timeout 120
python -m urbanpulse.tail_topic urbanpulse.health_advisories --messages 3 --timeout 120
find output/ward_energy -maxdepth 3 -type d | sort
```

On Windows, replace the final `find` command with:

```powershell
Get-ChildItem output\ward_energy -Directory -Recurse | Select-Object -ExpandProperty FullName
```

## 7. Report and final packaging

The visually verified report is in `report/UrbanPulse_Submission_Report.docx`; its PDF is `report/UrbanPulse_Submission_Report.pdf`. Live evidence is included for Kafka quorum, validation routing, enrichment, priority lag, all three Flink incidents, and both Spark outputs. Fill the cover fields, replace the route fixture, add the repository/video URLs and recording screenshots, then regenerate the documents before submission.

The packaging command refuses to create a ZIP while required evidence, cover fields, the official route schedule, or a current PDF is missing:

```text
# macOS
./scripts/submission-preflight.sh
./scripts/build-submission-zip.sh

# Windows PowerShell 7
.\scripts\submission-preflight.ps1
.\scripts\build-submission-zip.ps1
```

Do not commit `.env`, raw Kafka data, checkpoints, or large generated output. Commit the source, small CSV evidence, final report, and video link. The eLearn upload must be a single ZIP containing the report PDF, source repository/link, video link, and evidence.

## Repository map

- `infra/`: Kafka/Flink deployment and topic lifecycle
- `python/urbanpulse/`: Python producers, consumers, validation, enrichment, and evidence tools
- `flink/`: PyFlink keyed-state/event-time incident job
- `spark/`: PySpark ward energy and AQI advisory jobs
- `reference-data/`: replaceable fixtures
- `evidence/`: generated proof for rubric claims
- `report/`: report source, figure, DOCX/PDF builder, and final report
- `scripts/`: repeatable demonstrations and checks
- `PLATFORM_SETUP.md`: Windows/macOS setup and command mapping
