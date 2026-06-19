# start.ps1 -- Standalone startup: Temporal dev server + payment microservices
# No Docker, no observability stack required.
# Prerequisites: Java 21+, Maven, Temporal CLI

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

$TEMPORAL_PORT    = 7233
$TEMPORAL_UI_PORT = 8233
$INITIATION_PORT  = 8001
$WORKER_PORT      = 8020
$LOG_DIR          = "$PSScriptRoot\logs"
$DATA_DIR         = "$PSScriptRoot\data"
$SERVICES_DIR     = "$PSScriptRoot\services"
$PID_FILE         = "$PSScriptRoot\.pids.json"

# Locate the Temporal CLI:
# Checks PATH first, then local data\temporal-cli\, then common install locations.
$TEMPORAL_EXE = $null
if (Get-Command "temporal" -ErrorAction SilentlyContinue) {
    $TEMPORAL_EXE = "temporal"
} else {
    $candidates = @(
        "$PSScriptRoot\data\temporal-cli\temporal.exe",
        "$env:USERPROFILE\temporal.exe",
        "$env:USERPROFILE\temporal",
        "C:\ProgramData\chocolatey\bin\temporal.exe"
    )
    foreach ($c in $candidates) {
        $resolved = Resolve-Path $c -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($resolved -and (Test-Path $resolved.Path)) {
            $TEMPORAL_EXE = $resolved.Path
            $env:PATH = [System.IO.Path]::GetDirectoryName($resolved.Path) + ";" + $env:PATH
            break
        }
    }
}

function Write-Step($n, $msg) { Write-Host "[$n] $msg" -ForegroundColor Cyan }
function Write-OK($msg)       { Write-Host "    $msg" -ForegroundColor Green }

function Fail($msg) {
    Write-Host ""
    Write-Host "FATAL: $msg" -ForegroundColor Red
    exit 1
}

function Require-Command($cmd, $hint) {
    if (-not (Get-Command $cmd -ErrorAction SilentlyContinue)) {
        Write-Host "Missing required command: $cmd" -ForegroundColor Red
        if ($hint) { Write-Host "  $hint" -ForegroundColor Yellow }
        exit 1
    }
}

function Wait-ForHttp($url, $label, $timeoutSec = 90) {
    $deadline = (Get-Date).AddSeconds($timeoutSec)
    Write-Host "    Waiting for $label ..." -NoNewline
    while ((Get-Date) -lt $deadline) {
        try {
            $r = Invoke-WebRequest $url -TimeoutSec 3 -UseBasicParsing -ErrorAction SilentlyContinue
            if ($r -and $r.StatusCode -eq 200) {
                Write-Host " ready" -ForegroundColor Green
                return $true
            }
        } catch {}
        Write-Host "." -NoNewline
        Start-Sleep -Seconds 3
    }
    Write-Host " timed out" -ForegroundColor Yellow
    return $false
}

function Wait-ForTemporal($port, $timeoutSec = 90) {
    $deadline = (Get-Date).AddSeconds($timeoutSec)
    Write-Host "    Waiting for Temporal on port $port ..." -NoNewline
    while ((Get-Date) -lt $deadline) {
        try {
            $tcp = New-Object System.Net.Sockets.TcpClient
            $tcp.Connect("127.0.0.1", $port)
            $tcp.Close()
            Write-Host " ready" -ForegroundColor Green
            return $true
        } catch {}
        Write-Host "." -NoNewline
        Start-Sleep -Seconds 3
    }
    Write-Host " timed out" -ForegroundColor Yellow
    return $false
}

# Step 1: Prerequisites
Write-Step 1 "Checking prerequisites"

Require-Command "java" "Install Java 21+: https://adoptium.net"
Require-Command "mvn"  "Install Maven: https://maven.apache.org/download.cgi"

if (-not $TEMPORAL_EXE) {
    Write-Host "Missing: Temporal CLI (temporal.exe)" -ForegroundColor Red
    Write-Host "  Option 1: winget install Temporal.CLI" -ForegroundColor Yellow
    Write-Host "  Option 2: download from https://docs.temporal.io/cli and place in $PSScriptRoot\data\temporal-cli\" -ForegroundColor Yellow
    exit 1
}

# java/mvn write version info to stderr; capture in child scope to avoid Stop-mode error
$javaVer = & { $ErrorActionPreference = 'Continue'; java -version 2>&1 | Select-Object -First 1 }
$mvnVer  = & { $ErrorActionPreference = 'Continue'; mvn -v 2>&1 | Select-Object -First 1 }
$tempVer = & { $ErrorActionPreference = 'Continue'; & $TEMPORAL_EXE --version 2>&1 | Select-Object -First 1 }
Write-OK "java     : $javaVer"
Write-OK "mvn      : $mvnVer"
Write-OK "temporal : $tempVer  ($TEMPORAL_EXE)"

# Step 2: Prepare directories
New-Item -ItemType Directory -Force -Path $LOG_DIR  | Out-Null
New-Item -ItemType Directory -Force -Path $DATA_DIR | Out-Null

# Step 3: Maven build
Write-Step 2 "Building Maven modules (common, temporal-api, temporal-worker, payment-initiation)"
Write-Host "    Full output -> $LOG_DIR\maven-build.log"

$buildLog = "$LOG_DIR\maven-build.log"
& mvn -f "$SERVICES_DIR\pom.xml" `
      -pl "common,temporal-api,temporal-worker,payment-initiation" `
      -am clean package -DskipTests `
      2>&1 | Tee-Object -FilePath $buildLog | Where-Object { $_ -match "BUILD|ERROR " }
$mvnExit = $LASTEXITCODE

if ($mvnExit -ne 0) { Fail "Maven build failed -- see $buildLog" }
Write-OK "Build successful"

$initiationJar = Get-Item "$SERVICES_DIR\payment-initiation\target\payment-initiation-*.jar" -ErrorAction SilentlyContinue |
                 Where-Object { $_.Name -notmatch "sources|javadoc" } | Select-Object -First 1
$workerJar     = Get-Item "$SERVICES_DIR\temporal-worker\target\temporal-worker-*.jar" -ErrorAction SilentlyContinue |
                 Where-Object { $_.Name -notmatch "sources|javadoc" } | Select-Object -First 1

if (-not $initiationJar) { Fail "payment-initiation JAR not found under $SERVICES_DIR\payment-initiation\target\" }
if (-not $workerJar)     { Fail "temporal-worker JAR not found under $SERVICES_DIR\temporal-worker\target\" }

Write-OK "payment-initiation : $($initiationJar.Name)"
Write-OK "temporal-worker    : $($workerJar.Name)"

# Step 4: Temporal dev server
Write-Step 3 "Starting Temporal dev server (SQLite, persisted at data\temporal.db)"

$temporalProc = Start-Process $TEMPORAL_EXE `
    -ArgumentList @("server", "start-dev",
                    "--port",        "$TEMPORAL_PORT",
                    "--ui-port",     "$TEMPORAL_UI_PORT",
                    "--db-filename", "$DATA_DIR\temporal.db") `
    -RedirectStandardOutput "$LOG_DIR\temporal.log" `
    -RedirectStandardError  "$LOG_DIR\temporal-err.log" `
    -NoNewWindow -PassThru

if (-not (Wait-ForTemporal $TEMPORAL_PORT 90)) {
    Fail "Temporal did not become ready -- check $LOG_DIR\temporal.log"
}
Write-OK "Temporal running (PID $($temporalProc.Id))"

# Step 5: Spring Boot services
Write-Step 4 "Starting Spring Boot services"

$commonJvmArgs = @(
    "-Dtemporal.address=localhost:$TEMPORAL_PORT",
    "-Dtemporal.namespace=default"
)

$initiationProc = Start-Process "java" `
    -ArgumentList (@("-Dserver.port=$INITIATION_PORT") + $commonJvmArgs + @("-jar", $initiationJar.FullName)) `
    -RedirectStandardOutput "$LOG_DIR\payment-initiation.log" `
    -RedirectStandardError  "$LOG_DIR\payment-initiation-err.log" `
    -NoNewWindow -PassThru

$workerProc = Start-Process "java" `
    -ArgumentList (@("-Dserver.port=$WORKER_PORT") + $commonJvmArgs + @("-jar", $workerJar.FullName)) `
    -RedirectStandardOutput "$LOG_DIR\temporal-worker.log" `
    -RedirectStandardError  "$LOG_DIR\temporal-worker-err.log" `
    -NoNewWindow -PassThru

$initOk   = Wait-ForHttp "http://localhost:$INITIATION_PORT/actuator/health" "payment-initiation" 90
$workerOk = Wait-ForHttp "http://localhost:$WORKER_PORT/actuator/health"     "temporal-worker"    90

# Step 6: Save PIDs
@{
    temporal   = $temporalProc.Id
    initiation = $initiationProc.Id
    worker     = $workerProc.Id
} | ConvertTo-Json | Set-Content $PID_FILE -Encoding utf8

# Step 7: Summary
Write-Host ""
Write-Host "=======================================================" -ForegroundColor Green
Write-Host " Payment System Running" -ForegroundColor Green
Write-Host "=======================================================" -ForegroundColor Green
Write-Host ""
Write-Host "  Temporal UI    http://localhost:$TEMPORAL_UI_PORT"
Write-Host "  Payment API    http://localhost:$INITIATION_PORT/initiate  (POST)"
Write-Host "  Status query   http://localhost:$INITIATION_PORT/status/{uetr}"
Write-Host "  Manual approve http://localhost:$INITIATION_PORT/approve/{uetr}  (POST)"
Write-Host "  Worker health  http://localhost:$WORKER_PORT/actuator/health"
Write-Host ""
if (-not $initOk)   { Write-Host "  [!] payment-initiation may still be starting" -ForegroundColor Yellow }
if (-not $workerOk) { Write-Host "  [!] temporal-worker may still be starting" -ForegroundColor Yellow }
Write-Host "  Logs  $LOG_DIR"
Write-Host "  Stop  .\stop.ps1"
Write-Host ""
Write-Host "  PIDs  temporal=$($temporalProc.Id)  initiation=$($initiationProc.Id)  worker=$($workerProc.Id)"
Write-Host "=======================================================" -ForegroundColor Green
