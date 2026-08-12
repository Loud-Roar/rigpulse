$ErrorActionPreference = "Stop"

function Find-Python313 {
    try {
        & py -3.13 --version *> $null
        if ($LASTEXITCODE -eq 0) { return $true }
    } catch {}
    return $false
}

if (-not (Find-Python313)) {
    Write-Host "Python 3.13 is required for this build." -ForegroundColor Yellow
    Write-Host "Install it with: py install 3.13"
    exit 1
}

if (Test-Path ".venv") {
    $current = & .\.venv\Scripts\python.exe --version 2>&1
    if ($current -notmatch "Python 3\.13") {
        Remove-Item -Recurse -Force .venv
    }
}

if (!(Test-Path ".venv")) {
    & py -3.13 -m venv .venv
}

& .\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

$env:RIGPULSE_DATA_DIR="$PWD\data"
$env:RIGPULSE_POLL_SECONDS="10"

Write-Host ""
Write-Host "RigPulse is starting..." -ForegroundColor Cyan
Write-Host "Open: http://localhost:8080" -ForegroundColor Green
Write-Host ""

python -m uvicorn app.main:app --host 0.0.0.0 --port 8080
