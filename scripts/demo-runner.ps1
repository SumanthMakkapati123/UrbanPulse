# UrbanPulse Interactive Demo Runner for Windows PowerShell 7
# Run from urbanpulse-submission:  .\scripts\demo-runner.ps1

param(
    [string]$PythonRuntime = "python"
)

$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "common.ps1")
Set-UrbanPulsePythonPath

$env:KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094"

$script:BackgroundProcesses = @()

function Stop-BackgroundProcesses {
    if ($script:BackgroundProcesses.Count -gt 0) {
        Write-Host ""
        Write-Host "  Cleaning up background processes ..." -ForegroundColor Red
        foreach ($proc in $script:BackgroundProcesses) {
            if ($null -ne $proc -and -not $proc.HasExited) {
                Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
            }
        }
        $script:BackgroundProcesses = @()
        Write-Host "  Done." -ForegroundColor Green
    }
}

function Write-Banner([string]$title) {
    Write-Host ""
    Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host "  $title" -ForegroundColor Green
    Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host ""
}

function Write-Step([string]$message) {
    Write-Host ""
    Write-Host "  ▶ $message" -ForegroundColor Yellow
}

function Write-Cmd([string]$commandText) {
    Write-Host "  $ $commandText" -ForegroundColor DarkGray
}

function Pause-Demo {
    Write-Host ""
    Write-Host "  Press ENTER to continue …" -NoNewline -ForegroundColor White
    [void][System.Console]::ReadLine()
}

function Start-PyBg([string[]]$Arguments) {
    Write-Cmd "python -m $($Arguments -join ' ') &"
    $proc = Start-Process -FilePath $PythonRuntime -ArgumentList (@("-m") + $Arguments) `
        -WorkingDirectory $script:RepoRoot -NoNewWindow -PassThru `
        -RedirectStandardOutput "NUL" -RedirectStandardError "NUL"
    $script:BackgroundProcesses += $proc
}

function Invoke-PyRun([string[]]$Arguments) {
    Write-Cmd "python -m $($Arguments -join ' ')"
    $argsList = @("-m") + $Arguments
    & $PythonRuntime @argsList 2>&1 | Where-Object { $_ -notmatch '%[0-9]\|' }
}

try {
    ########################################################################
    Write-Banner "PART 1: ARCHITECTURE & KAFKA CLUSTER"
    ########################################################################

    $composeFile = Join-Path $script:RepoRoot "infra\docker-compose.yml"
    $runningBrokers = (docker compose -f $composeFile ps --status running 2>$null | Select-String "kafka" | Measure-Object).Count

    if ($runningBrokers -ge 3) {
        Write-Host "  ✔ Kafka cluster already running ($runningBrokers brokers detected) — skipping startup" -ForegroundColor Green
    } else {
        Write-Step "Starting 3-broker Kafka cluster …"
        Write-Cmd "docker compose -f infra\docker-compose.yml up -d kafka-1 kafka-2 kafka-3"
        docker compose -f $composeFile up -d kafka-1 kafka-2 kafka-3

        Write-Step "Waiting for KRaft election …"
        Start-Sleep -Seconds 20
    }

    Write-Step "Cluster status …"
    Write-Cmd "docker compose -f infra\docker-compose.yml ps"
    docker compose -f $composeFile ps

    Write-Step "KRaft quorum status …"
    Write-Cmd "kafka-metadata-quorum.sh --bootstrap-server kafka-1:9092 describe --status"
    docker compose -f $composeFile exec -T kafka-1 `/opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka-1:9092 describe --status

    Write-Step "Application topics (excluding internal topics) …"
    Write-Cmd "kafka-topics.sh --bootstrap-server kafka-1:9092 --describe"
    docker compose -f $composeFile exec -T kafka-1 `/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe | Where-Object { $_ -notmatch '__consumer_offsets|__transaction' }

    Pause-Demo

    ########################################################################
    Write-Banner "PART 2: INGESTION, DLQ & ENRICHMENT"
    ########################################################################

    Write-Step "Loading route schedule into Kafka …"
    Invoke-PyRun @("urbanpulse.load_route_schedule", "reference-data\route_schedule.csv")
    Write-Host "  ✔ Route schedule loaded" -ForegroundColor Green

    Write-Step "Starting route enrichment engine (background) …"
    Start-PyBg @("urbanpulse.route_enrichment", "--schedule-csv", "reference-data\route_schedule.csv")
    Start-Sleep -Seconds 3
    Write-Host "  ✔ Enrichment engine running" -ForegroundColor Green

    Write-Step "Starting producers (background) …"
    Start-PyBg @("urbanpulse.bus_gps_producer", "--events", "50000", "--rate", "200")
    Start-PyBg @("urbanpulse.traffic_signal_producer", "--events", "50000", "--rate", "200")
    Start-PyBg @("urbanpulse.air_quality_producer", "--events", "50000", "--rate", "60", "--null-rate", "0.05")
    Start-PyBg @("urbanpulse.smart_meter_producer", "--events", "50000", "--rate", "500")
    Write-Host "  ✔ 4 producers streaming data continuously" -ForegroundColor Green

    Write-Host "  Letting producers generate data for 30 seconds …"
    Start-Sleep -Seconds 30

    Write-Step "Tailing raw bus GPS events …"
    Invoke-PyRun @("urbanpulse.tail_topic", "urbanpulse.bus_gps", "--messages", "1")

    Write-Step "Tailing raw traffic signal events …"
    Invoke-PyRun @("urbanpulse.tail_topic", "urbanpulse.traffic_signals", "--messages", "1")

    Write-Step "Tailing raw air quality events …"
    Invoke-PyRun @("urbanpulse.tail_topic", "urbanpulse.air_quality", "--messages", "1")

    Write-Step "Tailing raw smart meter events …"
    Invoke-PyRun @("urbanpulse.tail_topic", "urbanpulse.smart_meters", "--messages", "1")

    Write-Step "Tailing enriched bus GPS events (with route schedule join) …"
    Invoke-PyRun @("urbanpulse.tail_topic", "urbanpulse.bus_gps_enriched", "--messages", "1")

    Write-Step "Starting producers with high error rates for DLQ demo …"
    Start-PyBg @("urbanpulse.air_quality_producer", "--events", "300", "--rate", "30", "--null-rate", "0.10", "--out-of-range-rate", "0.05")
    Start-PyBg @("urbanpulse.bus_gps_producer", "--events", "200", "--rate", "30", "--invalid-gps-rate", "0.10")
    Start-Sleep -Seconds 5

    Write-Step "Capturing DLQ report (15 seconds) …"
    Invoke-PyRun @("urbanpulse.dlq_report", "--duration", "15", "--output", "evidence\dlq-report.csv")
    Write-Host ""
    Write-Host "  --- DLQ Report ---" -ForegroundColor Cyan
    if (Test-Path "evidence\dlq-report.csv") {
        Get-Content "evidence\dlq-report.csv"
    } else {
        Write-Host "  (no DLQ events captured)"
    }

    Write-Step "Tailing DLQ topic (sample bad event envelope) …"
    Invoke-PyRun @("urbanpulse.tail_topic", "urbanpulse.dlq", "--messages", "1")

    Pause-Demo

    ########################################################################
    Write-Banner "PART 3: PRIORITY CONSUMERS & PYFLINK"
    ########################################################################

    Write-Step "Stopping producers …"
    Stop-BackgroundProcesses
    Start-Sleep -Seconds 2

    Write-Step "Running priority consumer lag demo …"
    Write-Cmd ".\scripts\run-priority-demo.ps1"
    & (Join-Path $PSScriptRoot "run-priority-demo.ps1")

    Write-Host ""
    Write-Host "  --- Consumer Lag Report ---" -ForegroundColor Cyan
    if (Test-Path "evidence\consumer-lag.csv") {
        Get-Content "evidence\consumer-lag.csv"
    } else {
        Write-Host "  (lag report not generated)"
    }

    Pause-Demo

    Write-Step "Starting Flink & submitting incident job …"
    Write-Cmd ".\scripts\run-flink-incidents.ps1"
    $flinkProc = Start-Process -FilePath "pwsh" -ArgumentList @("-File", (Join-Path $PSScriptRoot "run-flink-incidents.ps1")) `
        -WorkingDirectory $script:RepoRoot -NoNewWindow -PassThru `
        -RedirectStandardOutput "NUL" -RedirectStandardError "NUL"
    $script:BackgroundProcesses += $flinkProc

    Write-Host "  Waiting 15 seconds for Flink to start …"
    Start-Sleep -Seconds 15
    Write-Host ""
    Write-Host "  >>> Open Flink UI: http://localhost:8081 <<<" -ForegroundColor Green
    Write-Host ""

    Write-Step "Publishing incident test fixtures …"
    Invoke-PyRun @("urbanpulse.incident_fixture_producer")

    Write-Step "Tailing incident alerts …"
    Invoke-PyRun @("urbanpulse.tail_topic", "urbanpulse.incidents", "--messages", "2", "--timeout", "90")

    Pause-Demo

    ########################################################################
    Write-Banner "PART 4: PYSPARK ANALYTICS"
    ########################################################################

    Write-Step "Stopping Flink …"
    Stop-BackgroundProcesses
    Start-Sleep -Seconds 2

    Write-Step "Starting Spark ward energy aggregation …"
    Write-Cmd ".\scripts\run-spark-ward-energy.ps1"
    $sparkProc1 = Start-Process -FilePath "pwsh" -ArgumentList @("-File", (Join-Path $PSScriptRoot "run-spark-ward-energy.ps1")) `
        -WorkingDirectory $script:RepoRoot -NoNewWindow -PassThru `
        -RedirectStandardOutput "NUL" -RedirectStandardError "NUL"
    $script:BackgroundProcesses += $sparkProc1

    Write-Step "Starting Spark health advisories …"
    Write-Cmd ".\scripts\run-spark-health-advisories.ps1"
    $sparkProc2 = Start-Process -FilePath "pwsh" -ArgumentList @("-File", (Join-Path $PSScriptRoot "run-spark-health-advisories.ps1")) `
        -WorkingDirectory $script:RepoRoot -NoNewWindow -PassThru `
        -RedirectStandardOutput "NUL" -RedirectStandardError "NUL"
    $script:BackgroundProcesses += $sparkProc2

    Write-Host "  Waiting 15 seconds for Spark jobs …"
    Start-Sleep -Seconds 15

    Write-Step "Starting producers to feed Spark streams …"
    Start-PyBg @("urbanpulse.air_quality_producer", "--events", "50", "--rate", "10")
    Start-PyBg @("urbanpulse.smart_meter_producer", "--events", "200", "--rate", "100")
    Write-Host "  ✔ Producers running" -ForegroundColor Green

    Write-Step "Tailing ward energy summary …"
    Invoke-PyRun @("urbanpulse.tail_topic", "urbanpulse.ward_energy_summary", "--messages", "2", "--timeout", "90")

    Write-Step "Tailing health advisories …"
    Invoke-PyRun @("urbanpulse.tail_topic", "urbanpulse.health_advisories", "--messages", "2", "--timeout", "90")

    Write-Step "Showing Parquet output tree …"
    Write-Cmd "Get-ChildItem output\ward_energy -Directory -Recurse"
    if (Test-Path "output\ward_energy") {
        Get-ChildItem "output\ward_energy" -Directory -Recurse | Select-Object -ExpandProperty FullName
    } else {
        Write-Host "  (no Parquet output yet)"
    }

    Pause-Demo

    ########################################################################
    Write-Banner "DEMO COMPLETE — TEARING DOWN"
    ########################################################################

    Write-Step "Stopping all background processes …"
    Stop-BackgroundProcesses

    Write-Step "Stopping Spark containers …"
    docker stop urbanpulse-spark-ward 2>$null
    docker stop urbanpulse-spark-aqi 2>$null

    Write-Step "Stopping Docker compose services …"
    Write-Cmd "docker compose -f infra\docker-compose.yml --profile processing down"
    docker compose -f $composeFile --profile processing down 2>$null

    Write-Host ""
    Write-Host "  Demo finished successfully!" -ForegroundColor Green
    Write-Host ""
}
finally {
    Stop-BackgroundProcesses
}
