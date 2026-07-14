param(
    [ValidateSet("plan", "repair", "execute")]
    [string]$Mode = "plan",

    [Parameter(Mandatory = $true)]
    [ValidatePattern("^\d{4}-\d{2}-\d{2}$")]
    [string]$End,

    [ValidatePattern("^sha256:[0-9a-f]{64}$")]
    [string]$PlanId,

    [ValidateRange(0, 259)]
    [int]$MaxCalls,

    [ValidateRange(0, 1)]
    [int]$SplitCalls,

    [ValidateRange(0, 1)]
    [int]$DividendCalls,

    [ValidateRange(0, 257)]
    [int]$OpenCloseCalls,

    [ValidateSet("stockapi-msft-acquisition-only")]
    [string]$Authorization,

    [ValidatePattern("^[a-z0-9][a-z0-9._-]{2,63}$")]
    [string]$AuthorizationId
)

# One-command wrapper for the separately planned and authorized action+price lane.
# The API key is read from ignored .env and is never accepted on the command line.
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$mutex = [System.Threading.Mutex]::new($false, "Global\StockApiMutatingOperator")
$mutexHeld = $false
$commandExitCode = 0

Push-Location -LiteralPath $PSScriptRoot
try {
    try {
        $mutexHeld = $mutex.WaitOne(0)
    }
    catch [System.Threading.AbandonedMutexException] {
        $mutexHeld = $true
    }
    if (-not $mutexHeld) {
        throw "another vendor-acquisition wrapper is already running"
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
            throw "$name must be unset for the local vendor acquisition"
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
    if ($Mode -ne "plan") {
        $conflictingServices = @(
            $runningServices | Where-Object { $_ -in @("worker", "beat") }
        )
        if ($conflictingServices.Count -ne 0) {
            throw "stop the ordinary worker and beat before a mutating acquisition command"
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
            throw "stop native Celery processes before a mutating acquisition command"
        }
    }

    if ($Mode -eq "plan") {
        foreach ($name in @(
            "PlanId", "MaxCalls", "SplitCalls", "DividendCalls", "OpenCloseCalls",
            "Authorization", "AuthorizationId"
        )) {
            if ($PSBoundParameters.ContainsKey($name)) {
                throw "plan mode accepts only -Mode plan and -End"
            }
        }
        uv run python -m scripts.vendor_acquisition plan --end $End
        $commandExitCode = $LASTEXITCODE
    }
    elseif ($Mode -eq "repair") {
        foreach ($name in @(
            "MaxCalls", "SplitCalls", "DividendCalls", "OpenCloseCalls",
            "Authorization", "AuthorizationId"
        )) {
            if ($PSBoundParameters.ContainsKey($name)) {
                throw "repair mode requires only -PlanId and never authorizes vendor calls"
            }
        }
        if (-not $PSBoundParameters.ContainsKey("PlanId")) {
            throw "repair mode requires only -PlanId and never authorizes vendor calls"
        }
        uv run python -m scripts.vendor_acquisition repair --end $End --plan-id $PlanId
        $commandExitCode = $LASTEXITCODE
    }
    else {
        foreach ($name in @(
            "PlanId", "MaxCalls", "SplitCalls", "DividendCalls", "OpenCloseCalls",
            "Authorization", "AuthorizationId"
        )) {
            if (-not $PSBoundParameters.ContainsKey($name)) {
                throw "execute mode requires the plan, exact typed allocation, authorization, and ID"
            }
        }
        if ($MaxCalls -lt 1) {
            throw "execute mode requires a positive exact global call ceiling"
        }
        if ($MaxCalls -ne ($SplitCalls + $DividendCalls + $OpenCloseCalls)) {
            throw "MaxCalls must equal SplitCalls + DividendCalls + OpenCloseCalls"
        }
        uv run python -m scripts.vendor_acquisition execute `
            --end $End `
            --plan-id $PlanId `
            --max-calls $MaxCalls `
            --split-calls $SplitCalls `
            --dividend-calls $DividendCalls `
            --open-close-calls $OpenCloseCalls `
            --authorization $Authorization `
            --authorization-id $AuthorizationId
        $commandExitCode = $LASTEXITCODE
    }
}
finally {
    if ($mutexHeld) {
        $mutex.ReleaseMutex()
    }
    $mutex.Dispose()
    Pop-Location
}

if ($commandExitCode -ne 0) {
    exit $commandExitCode
}
