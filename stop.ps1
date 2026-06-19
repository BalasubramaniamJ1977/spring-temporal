# stop.ps1 -- Stop all payment system processes started by start.ps1

$PID_FILE = "$PSScriptRoot\.pids.json"

if (-not (Test-Path $PID_FILE)) {
    Write-Host "No .pids.json found -- nothing to stop." -ForegroundColor Yellow
    exit 0
}

$pids = Get-Content $PID_FILE -Raw | ConvertFrom-Json

$entries = @(
    @{ Name = "payment-initiation"; Id = $pids.initiation },
    @{ Name = "temporal-worker";    Id = $pids.worker },
    @{ Name = "temporal";           Id = $pids.temporal }
)

foreach ($e in $entries) {
    if (-not $e.Id) { continue }
    $proc = Get-Process -Id $e.Id -ErrorAction SilentlyContinue
    if ($proc) {
        Write-Host "Stopping $($e.Name) (PID $($e.Id)) ..."
        Stop-Process -Id $e.Id -Force -ErrorAction SilentlyContinue
        Write-Host "  stopped" -ForegroundColor Green
    } else {
        Write-Host "$($e.Name) (PID $($e.Id)) is not running"
    }
}

Remove-Item $PID_FILE -Force -ErrorAction SilentlyContinue
Write-Host ""
Write-Host "All services stopped." -ForegroundColor Green
