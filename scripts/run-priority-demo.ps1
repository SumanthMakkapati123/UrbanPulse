param([string]$PythonRuntime = "python")

. (Join-Path $PSScriptRoot "common.ps1")
Set-UrbanPulsePythonPath

$rawDir = Join-Path $script:RepoRoot "evidence/raw"
New-Item -ItemType Directory -Force -Path $rawDir | Out-Null
$processes = @()

function Start-UrbanPulseProcess {
    param([string[]]$Arguments, [string]$LogName)
    $stdout = Join-Path $rawDir "$LogName.out.log"
    $stderr = Join-Path $rawDir "$LogName.err.log"
    return Start-Process -FilePath $PythonRuntime -ArgumentList $Arguments `
        -WorkingDirectory $script:RepoRoot -NoNewWindow -PassThru `
        -RedirectStandardOutput $stdout -RedirectStandardError $stderr
}

try {
    $processes += Start-UrbanPulseProcess `
        @("-m", "urbanpulse.priority_consumer", "--priority", "high", "--consumer-id", "high-1") `
        "high-priority"
    foreach ($consumerId in 1..3) {
        $processes += Start-UrbanPulseProcess `
            @("-m", "urbanpulse.priority_consumer", "--priority", "standard", "--consumer-id", "standard-$consumerId", "--delay-ms", "250") `
            "standard-$consumerId"
    }
    $processes += Start-UrbanPulseProcess `
        @("-m", "urbanpulse.traffic_signal_producer", "--events", "30000", "--rate", "380") `
        "traffic-producer"

    & (Join-Path $PSScriptRoot "capture-consumer-lag.ps1") -DurationSeconds 120
    if ($LASTEXITCODE -ne 0) { throw "Lag capture failed" }
}
finally {
    foreach ($process in $processes) {
        if ($null -ne $process -and -not $process.HasExited) {
            Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
        }
    }
}
