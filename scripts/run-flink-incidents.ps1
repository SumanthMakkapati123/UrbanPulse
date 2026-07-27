. (Join-Path $PSScriptRoot "common.ps1")

$composeFile = Join-Path $script:RepoRoot "infra/docker-compose.yml"
Invoke-Checked "docker" @(
    "compose", "-f", $composeFile, "--profile", "processing", "up", "-d", "--build",
    "kafka-1", "kafka-2", "kafka-3", "flink-jobmanager", "flink-taskmanager"
)
Invoke-Checked "docker" @(
    "compose", "-f", $composeFile, "exec", "-T", "flink-jobmanager",
    "/opt/flink/bin/flink", "run", "-d", "--python", "/opt/flink/jobs/incident_detection.py"
)
