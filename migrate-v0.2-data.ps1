param(
    [Parameter(Mandatory=$true)]
    [string]$OldDataDir
)

$ErrorActionPreference = "Stop"

$source = Join-Path $OldDataDir "hashwatcher.db"
$targetDir = Join-Path $PSScriptRoot "data"
$target = Join-Path $targetDir "rigpulse.db"

if (-not (Test-Path $source)) {
    Write-Host "Could not find legacy database: $source" -ForegroundColor Red
    exit 1
}

New-Item -ItemType Directory -Path $targetDir -Force | Out-Null

if (Test-Path $target) {
    $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    Copy-Item $target "$target.backup-$stamp"
    Write-Host "Existing RigPulse database backed up." -ForegroundColor Yellow
}

Copy-Item $source $target -Force
Write-Host "Imported miners/history/settings into: $target" -ForegroundColor Green
