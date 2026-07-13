param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("plan", "repair", "execute")]
    [string]$Mode,

    [Parameter(Mandatory = $true)]
    [ValidatePattern("^\d{4}-\d{2}-\d{2}$")]
    [string]$End,

    [ValidatePattern("^sha256:[0-9a-f]{64}$")]
    [string]$PlanId,

    [ValidateRange(1, 258)]
    [int]$MaxCalls,

    [ValidateSet("stockapi-msft-backfill-only")]
    [string]$Authorization,

    [ValidatePattern("^[a-z0-9][a-z0-9._-]{2,63}$")]
    [string]$AuthorizationId
)

# One-command wrapper for the separately planned and authorized MSFT backfill.
# The API key is read from ignored .env and is never accepted on the command line.
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$mutex = [System.Threading.Mutex]::new($false, "Global\StockApiVendorBackfill")
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
        throw "another vendor-backfill wrapper is already running"
    }
    if (-not (Test-Path -LiteralPath ".env")) {
        throw ".env is required"
    }
    $runningServices = @(docker compose ps --status running --services)
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
            throw "stop the ordinary worker and beat before a mutating backfill command"
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
            throw "stop native Celery processes before a mutating backfill command"
        }
    }

    if ($Mode -eq "plan") {
        if ($PlanId -or $MaxCalls -or $Authorization -or $AuthorizationId) {
            throw "plan mode accepts only -Mode plan and -End"
        }
        uv run python -m scripts.vendor_backfill plan --end $End
        $commandExitCode = $LASTEXITCODE
    }
    elseif ($Mode -eq "repair") {
        if (-not $PlanId -or $MaxCalls -or $Authorization -or $AuthorizationId) {
            throw "repair mode requires only -PlanId and never authorizes vendor calls"
        }
        uv run python -m scripts.vendor_backfill repair --end $End --plan-id $PlanId
        $commandExitCode = $LASTEXITCODE
    }
    else {
        if (-not $PlanId -or $MaxCalls -lt 1 -or -not $Authorization -or -not $AuthorizationId) {
            throw "execute mode requires -PlanId, -MaxCalls, -Authorization, and -AuthorizationId"
        }
        uv run python -m scripts.vendor_backfill execute `
            --end $End `
            --plan-id $PlanId `
            --max-calls $MaxCalls `
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
