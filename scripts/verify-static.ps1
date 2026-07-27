param([string]$PythonRuntime = "python")

. (Join-Path $PSScriptRoot "common.ps1")
Set-Location $script:RepoRoot

$previousPythonPath = $env:PYTHONPATH
try {
    $env:PYTHONPATH = Join-Path $script:RepoRoot "python"
    Invoke-Checked $PythonRuntime @("-m", "unittest", "discover", "-s", "python/tests", "-v")
    $env:PYTHONPATH = Join-Path $script:RepoRoot "flink"
    Invoke-Checked $PythonRuntime @("-m", "unittest", "discover", "-s", "flink/tests", "-v")
    Invoke-Checked $PythonRuntime @("-m", "compileall", "-q", "python", "flink", "spark")
    Invoke-Checked $PythonRuntime @("scripts/verify_platform.py")
    Invoke-Checked "docker" @("compose", "-f", "infra/docker-compose.yml", "config", "--quiet")
}
finally {
    $env:PYTHONPATH = $previousPythonPath
}

Write-Host "Static verification passed"
