. (Join-Path $PSScriptRoot "common.ps1")

$sparkDir = Join-Path $script:RepoRoot "spark"
$referenceDir = Join-Path $script:RepoRoot "reference-data"
$checkpointDir = Join-Path $script:RepoRoot "checkpoints"
New-Item -ItemType Directory -Force -Path $checkpointDir | Out-Null

Invoke-Checked "docker" @(
    "run", "--rm", "--name", "urbanpulse-spark-aqi", "--network", "urbanpulse_default",
    "-e", "KAFKA_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092,kafka-3:9092",
    "--mount", "type=bind,source=$sparkDir,target=/opt/urbanpulse/spark,readonly",
    "--mount", "type=bind,source=$referenceDir,target=/opt/urbanpulse/reference-data,readonly",
    "--mount", "type=bind,source=$checkpointDir,target=/opt/urbanpulse/checkpoints",
    "spark:4.1.2-python3",
    "/opt/spark/bin/spark-submit", "--master", "local[4]",
    "--conf", "spark.jars.ivy=/tmp/.ivy2",
    "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2",
    "/opt/urbanpulse/spark/aqi_health_advisories.py"
)
