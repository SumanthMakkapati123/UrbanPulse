. (Join-Path $PSScriptRoot "../scripts/common.ps1")

$composeFile = Join-Path $PSScriptRoot "docker-compose.yml"
Invoke-Checked "docker" @("compose", "-f", $composeFile, "ps")
Invoke-Checked "docker" @(
    "compose", "-f", $composeFile, "exec", "-T", "kafka-1",
    "/opt/kafka/bin/kafka-metadata-quorum.sh", "--bootstrap-server", "kafka-1:9092",
    "describe", "--status"
)
Invoke-Checked "docker" @(
    "compose", "-f", $composeFile, "exec", "-T", "kafka-1",
    "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka-1:9092", "--describe"
)
