param([int]$DurationSeconds = 120)

. (Join-Path $PSScriptRoot "common.ps1")

$composeFile = Join-Path $script:RepoRoot "infra/docker-compose.yml"
$output = Join-Path $script:RepoRoot "evidence/consumer-lag.csv"
$groups = @("traffic-signals-high-priority", "traffic-signals-standard-priority")
Write-Utf8NoBom $output "timestamp_utc,group,total_lag`r`n"

$deadline = [DateTime]::UtcNow.AddSeconds($DurationSeconds)
while ([DateTime]::UtcNow -lt $deadline) {
    $timestamp = [DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ")
    foreach ($group in $groups) {
        $lines = & docker compose -f $composeFile exec -T kafka-1 `
            /opt/kafka/bin/kafka-consumer-groups.sh `
            --bootstrap-server kafka-1:9092 --describe --group $group 2>$null
        if ($LASTEXITCODE -ne 0) {
            throw "Unable to read consumer lag for $group"
        }
        $totalLag = 0
        foreach ($line in $lines) {
            $columns = ($line.Trim() -split "\s+")
            if ($columns.Count -gt 5 -and $columns[5] -match "^\d+$") {
                $totalLag += [int64]$columns[5]
            }
        }
        [System.IO.File]::AppendAllText(
            $output,
            "$timestamp,$group,$totalLag`r`n",
            [System.Text.UTF8Encoding]::new($false)
        )
    }
    Start-Sleep -Seconds 5
}

Write-Host "Wrote $output"
