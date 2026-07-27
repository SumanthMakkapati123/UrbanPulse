param([string]$PythonRuntime = "python")

. (Join-Path $PSScriptRoot "common.ps1")
Set-Location $script:RepoRoot

function Fail-Preflight([string]$Message) {
    throw "Submission preflight failed: $Message"
}

$requiredFiles = @(
    "report/UrbanPulse_Submission_Report.docx",
    "report/UrbanPulse_Submission_Report.pdf",
    "evidence/cluster-verification.txt",
    "evidence/consumer-lag.csv",
    "evidence/priority-consumer-lag.png",
    "evidence/dlq-report.csv",
    "evidence/enriched-event.json",
    "evidence/incidents.jsonl",
    "evidence/ward-energy-sample.json",
    "evidence/parquet-partitions.txt",
    "evidence/health-advisory-sample.json"
)
foreach ($path in $requiredFiles) {
    $item = Get-Item $path -ErrorAction SilentlyContinue
    if ($null -eq $item -or $item.Length -eq 0) { Fail-Preflight "missing or empty $path" }
}

$reportText = Get-Content "report/report.md" -Raw
if ($reportText -match '(?m)^\*\*(Student name\(s\)|Student ID\(s\)|Git repository|Video walkthrough):\*\* _+') {
    Fail-Preflight "fill every identity, Git repository, and video field in report/report.md"
}

$fixtureSha = "D9FA9F86C9EA1000E30DFEEDD75996A6F217F4F002990AED585B6FCD5099E0FC"
$currentSha = (Get-FileHash "reference-data/route_schedule.csv" -Algorithm SHA256).Hash
if ($currentSha -eq $fixtureSha) {
    Fail-Preflight "replace the development route_schedule.csv with the official eLearn file"
}

$dlqText = Get-Content "evidence/dlq-report.csv" -Raw
if ($dlqText -notmatch '(?m)^duration_seconds,300\r?$') {
    Fail-Preflight "evidence/dlq-report.csv is not a 300-second capture"
}

if ((Get-Item "report/report.md").LastWriteTimeUtc -gt (Get-Item "report/UrbanPulse_Submission_Report.pdf").LastWriteTimeUtc) {
    Fail-Preflight "report PDF is older than report/report.md; rebuild and visually verify it"
}

& (Join-Path $PSScriptRoot "verify-static.ps1") -PythonRuntime $PythonRuntime
if ($LASTEXITCODE -ne 0) { Fail-Preflight "static verification failed" }
Write-Host "Submission preflight passed"
