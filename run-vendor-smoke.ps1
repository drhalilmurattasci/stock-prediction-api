param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern("^\d{4}-\d{2}-\d{2}$")]
    [string]$Session,

    [Parameter(Mandatory = $true)]
    [ValidateSet("stockapi-vendor-smoke-only")]
    [string]$Authorization
)

# One-command wrapper for the separately authorized one-attempt vendor smoke.
# The API key is read from ignored .env and is never accepted on the command line.
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
# Global\ scope: exclusion must hold machine-wide across logon sessions
# (console + RDP + scheduled task), not just within one session.
$mutex = [System.Threading.Mutex]::new($false, "Global\StockApiMutatingOperator")
$mutexHeld = $false

Push-Location -LiteralPath $PSScriptRoot
try {
    try {
        $mutexHeld = $mutex.WaitOne(0)
    }
    catch [System.Threading.AbandonedMutexException] {
        $mutexHeld = $true
    }
    if (-not $mutexHeld) {
        throw "another vendor-smoke wrapper is already running"
    }
    if (-not (Test-Path -LiteralPath ".env")) {
        throw ".env is required"
    }
    foreach ($name in @(
        "COMPOSE_FILE",
        "COMPOSE_PROFILES",
        "COMPOSE_PROJECT_NAME",
        "COMPOSE_ENV_FILES",
        "COMPOSE_DISABLE_ENV_FILE",
        "DOCKER_CONTEXT",
        "DOCKER_HOST"
    )) {
        if (-not [string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($name, "Process"))) {
            throw "$name must be unset for the local vendor smoke"
        }
    }
    $dockerContext = ([string](docker context show)).Trim()
    if ($LASTEXITCODE -ne 0 -or $dockerContext -cne "desktop-linux") {
        throw "Docker must use the local desktop-linux context"
    }
    $dockerEndpoint = ([string](
        docker context inspect "desktop-linux" --format "{{.Endpoints.docker.Host}}"
    )).Trim()
    if ($LASTEXITCODE -ne 0 -or $dockerEndpoint -cne "npipe:////./pipe/dockerDesktopLinuxEngine") {
        throw "Docker must use the local Docker Desktop Linux endpoint"
    }
    $dockerArgs = @("--context", "desktop-linux")
    $dockerIdentity = ([string](
        docker @dockerArgs info --format "{{.Name}}|{{.OperatingSystem}}"
    )).Trim()
    if ($LASTEXITCODE -ne 0 -or $dockerIdentity -cne "docker-desktop|Docker Desktop") {
        throw "the local Docker Desktop Linux daemon is unavailable"
    }
    $composeFile = (Resolve-Path -LiteralPath "docker-compose.yml").Path
    $envFile = (Resolve-Path -LiteralPath ".env").Path
    $composeArgs = @(
        "--env-file", $envFile,
        "--file", $composeFile,
        "--project-directory", $PSScriptRoot,
        "--project-name", "stock-api"
    )
    $runningServices = @(
        docker @dockerArgs compose @composeArgs ps --status running --services
    )
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose status check failed"
    }
    if ($runningServices -notcontains "timescaledb") {
        throw "timescaledb must already be running"
    }
    $conflictingServices = @($runningServices | Where-Object { $_ -in @("worker", "beat") })
    if ($conflictingServices.Count -ne 0) {
        throw "stop the ordinary worker and beat before the one-attempt smoke"
    }
    $workerProcessNamePattern = "^(?:py|python(?:w|[0-9]+(?:\.[0-9]+)?)?|celery)(?:\.exe)?$"
    $nativeWorkers = @(
        Get-CimInstance Win32_Process |
            Where-Object {
                $_.ProcessId -ne $PID -and
                $_.Name -match $workerProcessNamePattern -and
                $_.CommandLine -match "(?i)\bcelery\b"
            }
    )
    if ($nativeWorkers.Count -ne 0) {
        throw "stop native Celery processes before the one-attempt smoke"
    }
    uv run python -m scripts.vendor_smoke `
        --session $Session `
        --authorization $Authorization
    if ($LASTEXITCODE -ne 0) {
        throw "live vendor smoke failed"
    }
}
finally {
    if ($mutexHeld) {
        $mutex.ReleaseMutex()
    }
    $mutex.Dispose()
    Pop-Location
}
