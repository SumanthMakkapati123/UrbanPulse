. (Join-Path $PSScriptRoot "../scripts/common.ps1")

$composeFile = Join-Path $PSScriptRoot "docker-compose.yml"
$bootstrapServer = "kafka-1:9092"
$topics = @(
    @("urbanpulse.bus_gps", "12", "86400000", "delete"),
    @("urbanpulse.traffic_signals", "6", "604800000", "delete"),
    @("urbanpulse.air_quality", "3", "7776000000", "delete"),
    @("urbanpulse.smart_meters", "12", "31536000000", "delete"),
    @("urbanpulse.route_schedule", "3", "-1", "compact"),
    @("urbanpulse.bus_gps_enriched", "12", "86400000", "delete"),
    @("urbanpulse.incidents", "6", "2592000000", "delete"),
    @("urbanpulse.ward_energy_summary", "6", "31536000000", "delete"),
    @("urbanpulse.health_advisories", "3", "7776000000", "delete"),
    @("urbanpulse.dlq", "6", "7776000000", "delete")
)

foreach ($topic in $topics) {
    Invoke-Checked "docker" @(
        "compose", "-f", $composeFile, "exec", "-T", "kafka-1",
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server", $bootstrapServer,
        "--create", "--if-not-exists",
        "--topic", $topic[0],
        "--partitions", $topic[1],
        "--replication-factor", "3",
        "--config", "retention.ms=$($topic[2])",
        "--config", "cleanup.policy=$($topic[3])",
        "--config", "min.insync.replicas=2"
    )
}

Invoke-Checked "docker" @(
    "compose", "-f", $composeFile, "exec", "-T", "kafka-1",
    "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", $bootstrapServer, "--list"
)
