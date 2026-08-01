# Windows and macOS setup

UrbanPulse runs its infrastructure and stream processors in Linux containers, so Kafka, Flink, and Spark behave the same on Windows and macOS. Host-specific scripts are supplied only for shell syntax, path handling, process management, evidence capture, and packaging.

## Supported host environments

| Host | Required shell and container mode | Notes |
|---|---|---|
| Windows 10/11 | PowerShell 7 (`pwsh`), Docker Desktop using the WSL 2 backend and Linux containers | Enable Docker integration for the WSL distribution if the repository is stored inside WSL. Do not switch Docker Desktop to Windows containers. |
| macOS 13+ | Terminal with `zsh` or `bash`, Docker Desktop | Intel and Apple Silicon Macs are supported by the pinned multi-architecture images. |

Install Python 3.11 or 3.12, Git, Docker Desktop with Compose v2, and allocate at least 12 GB memory and 4 CPUs to Docker Desktop. Keep 15-20 GB disk space free for images, Kafka volumes, Maven packages, checkpoints, and test output.

The repository path may contain spaces. All supplied scripts resolve and quote the repository root; do not remove the quoting from examples.

## Windows PowerShell 7

Open PowerShell in the repository root. A process-scoped execution-policy override is sufficient if local scripts are blocked; it does not modify the machine-wide policy.

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r python\requirements.txt
$env:PYTHONPATH = (Join-Path $PWD "python")
docker version
docker compose version
.\scripts\verify-static.ps1
```

Start Kafka, create topics, and capture verification evidence:

```powershell
docker compose -f infra\docker-compose.yml up -d kafka-1 kafka-2 kafka-3
.\infra\create-topics.ps1
.\infra\verify-cluster.ps1 2>&1 | Tee-Object -FilePath evidence\cluster-verification.txt
```

The Python module commands in the main README work unchanged in PowerShell. For example:

```powershell
python -m urbanpulse.load_route_schedule reference-data\route_schedule.csv
python -m urbanpulse.air_quality_producer --events 1000 --rate 60 --null-rate 0.05
python -m urbanpulse.dlq_report --duration 300 --output evidence\dlq-report.csv
```

## macOS Terminal

Open Terminal in the repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r python/requirements.txt
export PYTHONPATH="$PWD/python"
docker version
docker compose version
./scripts/verify-static.sh
```

Start Kafka, create topics, and capture verification evidence:

```bash
docker compose -f infra/docker-compose.yml up -d kafka-1 kafka-2 kafka-3
./infra/create-topics.sh
./infra/verify-cluster.sh | tee evidence/cluster-verification.txt
```

If macOS removes executable bits after copying the folder outside Git, restore them once with `chmod +x scripts/*.sh infra/*.sh`.

## Equivalent commands

| Operation | macOS | Windows PowerShell 7 |
|---|---|---|
| Static verification | `./scripts/verify-static.sh` | `.\scripts\verify-static.ps1` |
| Create Kafka topics | `./infra/create-topics.sh` | `.\infra\create-topics.ps1` |
| Verify Kafka cluster | `./infra/verify-cluster.sh` | `.\infra\verify-cluster.ps1` |
| Interactive demo runner | `./scripts/demo-runner.sh` | `.\scripts\demo-runner.ps1` |
| Priority demonstration | `./scripts/run-priority-demo.sh` | `.\scripts\run-priority-demo.ps1` |
| Capture lag only | `./scripts/capture-consumer-lag.sh 120` | `.\scripts\capture-consumer-lag.ps1 -DurationSeconds 120` |
| Start PyFlink job | `./scripts/run-flink-incidents.sh` | `.\scripts\run-flink-incidents.ps1` |
| Run Spark energy job | `./scripts/run-spark-ward-energy.sh` | `.\scripts\run-spark-ward-energy.ps1` |
| Run Spark AQI job | `./scripts/run-spark-health-advisories.sh` | `.\scripts\run-spark-health-advisories.ps1` |
| Submission preflight | `./scripts/submission-preflight.sh` | `.\scripts\submission-preflight.ps1` |
| Build final ZIP | `./scripts/build-submission-zip.sh` | `.\scripts\build-submission-zip.ps1` |

The Spark PowerShell launchers use Docker `--mount` syntax so Windows drive letters and repository paths containing spaces are passed as one argument. Compose declares the project name `urbanpulse`, giving both hosts the same `urbanpulse_default` network used by the Spark containers.

## Stop and resume

Kafka and Flink use named Docker volumes. Stopping services preserves Kafka data and Flink checkpoints:

```powershell
docker compose -f infra\docker-compose.yml --profile processing stop
```

```bash
docker compose -f infra/docker-compose.yml --profile processing stop
```

Use `docker compose ... down` only when you want to remove containers and the network. Do not add `--volumes` unless the stored Kafka evidence and checkpoints may be deleted.

## Platform troubleshooting

- Confirm Docker Desktop is running before any verification script. On Windows, confirm it reports Linux containers and the WSL 2 engine.
- If a Windows bind mount is denied, enable file sharing for the repository drive in Docker Desktop or keep the clone inside the Docker-integrated WSL distribution.
- If ports 8081 or 19092-19094 are occupied, stop the conflicting local service; changing the ports also requires updating the documented inspection URLs/bootstrap addresses.
- First Spark execution downloads Kafka connector packages. Allow Docker network access and retry after the download completes.
- `.gitattributes` enforces LF for Bash/Docker files and CRLF on Windows checkout for PowerShell scripts, preventing shell errors caused by mixed line endings.
