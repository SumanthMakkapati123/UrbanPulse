param(
    [string]$Output,
    [string]$PythonRuntime = "python"
)

. (Join-Path $PSScriptRoot "common.ps1")
if ([string]::IsNullOrWhiteSpace($Output)) {
    $Output = Join-Path $script:RepoRoot "UrbanPulse_Submission.zip"
}
elseif (-not [System.IO.Path]::IsPathRooted($Output)) {
    $Output = [System.IO.Path]::GetFullPath((Join-Path (Get-Location) $Output))
}
if (Test-Path $Output) { throw "Refusing to overwrite existing file: $Output" }

& (Join-Path $PSScriptRoot "submission-preflight.ps1") -PythonRuntime $PythonRuntime
if ($LASTEXITCODE -ne 0) { throw "Submission preflight failed" }

$stageDir = Join-Path ([System.IO.Path]::GetTempPath()) ("urbanpulse-submission-" + [Guid]::NewGuid())
$packageDir = Join-Path $stageDir "UrbanPulse_Submission"
New-Item -ItemType Directory -Force -Path $packageDir | Out-Null

try {
    $excludedDirectories = @(".git", ".venv", "__pycache__", "checkpoints", "output", "raw")
    Get-ChildItem -Path $script:RepoRoot -Recurse -File | ForEach-Object {
        $relative = [System.IO.Path]::GetRelativePath($script:RepoRoot, $_.FullName)
        $segments = $relative -split '[\\/]'
        $skipDirectory = @($segments | Where-Object {
            $_ -in $excludedDirectories -or $_ -like "rendered*"
        }).Count -gt 0
        $skipFile = $_.Name -eq ".env" -or $_.Extension -in @(".pyc", ".zip") -or `
            $_.Name -like "a11y-report*.json"
        if (-not $skipDirectory -and -not $skipFile) {
            $destination = Join-Path $packageDir $relative
            New-Item -ItemType Directory -Force -Path (Split-Path $destination -Parent) | Out-Null
            Copy-Item $_.FullName $destination
        }
    }
    Compress-Archive -Path $packageDir -DestinationPath $Output -CompressionLevel Optimal
}
finally {
    if (Test-Path $stageDir) { Remove-Item -Recurse -Force $stageDir }
}

Write-Host "Wrote $Output"
