. (Join-Path $PSScriptRoot "common.ps1")

$sparkDir = Join-Path $script:RepoRoot "spark"
$outputDir = Join-Path $script:RepoRoot "output"
$checkpointDir = Join-Path $script:RepoRoot "checkpoints"
New-Item -ItemType Directory -Force -Path $outputDir, $checkpointDir | Out-Null

Invoke-Checked "docker" @(
    "run", "--rm", "--name", "urbanpulse-spark-ward", "--network", "urbanpulse_default",
    "-e", "KAFKA_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092,kafka-3:9092",
    "--mount", "type=bind,source=$sparkDir,target=/opt/urbanpulse/spark,readonly",
    "--mount", "type=bind,source=$outputDir,target=/opt/urbanpulse/output",
    "--mount", "type=bind,source=$checkpointDir,target=/opt/urbanpulse/checkpoints",
    "spark:4.1.2-python3",
    "/opt/spark/bin/spark-submit", "--master", "local[4]",
    "--conf", "spark.jars.ivy=/tmp/.ivy2",
    "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2",
    "/opt/urbanpulse/spark/ward_energy_stream.py"
)
