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

    [ValidatePattern("^sha256:[0-9a-f]{64}$")]
    [string]$CampaignId,

    [ValidateRange(0, 259)]
    [int]$CampaignBudgetDelta,

    [ValidateSet("stockapi-msft-acquisition-only")]
    [string]$Authorization,

    [ValidatePattern("^[a-z0-9][a-z0-9._-]{2,63}$")]
    [string]$AuthorizationId
)

# One-command wrapper for the separately planned and authorized action+price lane.
# Vendor credentials are loaded only from ignored .env and are never accepted on
# the command line or inherited from the caller's process environment.
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$mutex = [System.Threading.Mutex]::new($false, "Global\StockApiMutatingOperator")
$mutexHeld = $false
$commandExitCode = 0
$buildContext = $null
$buildContextRegistered = $false
$worktreeRoot = $null
$expectedTimescaleImage = "timescale/timescaledb:2.28.2-pg17"
$expectedMigrationHead = "0014_vendor_campaign_anchor"
$maxRecoveryCalls = 5
$maxCampaignCalls = 264

$vendorVariableNames = @(
    "POLYGON_API_KEY",
    "FMP_API_KEY",
    "FINNHUB_API_KEY",
    "NASDAQ_DATA_LINK_API_KEY",
    "ALPACA_API_KEY",
    "ALPACA_API_SECRET",
    "DATABENTO_API_KEY"
)
$proxyVariableNames = @("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY")
$pythonRoutingVariableNames = @("PYTHONHOME", "PYTHONPATH")
$uvRoutingVariableNames = @(
    "UV_ENV_FILE",
    "UV_PROJECT",
    "UV_WORKING_DIR",
    "UV_PYTHON",
    "UV_NO_SYNC",
    "UV_CONFIG_FILE",
    "UV_PROJECT_ENVIRONMENT",
    "VIRTUAL_ENV"
)
$scopedVariableNames = @(
    $vendorVariableNames + $proxyVariableNames + $pythonRoutingVariableNames +
        $uvRoutingVariableNames
)
$priorScopedVariables = @{}
foreach ($name in $scopedVariableNames) {
    $priorScopedVariables[$name] = [Environment]::GetEnvironmentVariable($name, "Process")
}

function Get-StockApiRunningServices {
    $services = @(docker @script:dockerArgs compose @script:composeArgs ps --status running --services)
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose status check failed"
    }
    return $services
}

function Assert-TimescaleContainer {
    param([string[]]$RunningServices)

    if ($RunningServices -notcontains "timescaledb") {
        throw "timescaledb must already be running"
    }
    $inspectLines = @(docker @script:dockerArgs inspect stockapi-timescaledb)
    if ($LASTEXITCODE -ne 0 -or $inspectLines.Count -eq 0) {
        throw "could not inspect the stockapi-timescaledb container"
    }
    try {
        $containers = @(ConvertFrom-Json -InputObject ($inspectLines -join "`n"))
    }
    catch {
        throw "the stockapi-timescaledb inspection result is malformed"
    }
    if ($containers.Count -ne 1) {
        throw "exactly one stockapi-timescaledb container is required"
    }
    $container = $containers[0]
    if (
        $container.Id -cnotmatch "^[0-9a-f]{64}$" -or
        $container.Name -cne "/stockapi-timescaledb" -or
        $container.Config.Image -cne $script:expectedTimescaleImage -or
        $container.Config.Labels.'com.docker.compose.project' -cne "stock-api" -or
        $container.Config.Labels.'com.docker.compose.service' -cne "timescaledb" -or
        $container.State.Running -cne $true -or
        $container.State.Health.Status -cne "healthy"
    ) {
        throw "timescaledb is not the exact healthy stock-api compose service"
    }
    $portProperties = @($container.NetworkSettings.Ports.PSObject.Properties)
    if ($portProperties.Count -ne 1 -or $portProperties[0].Name -cne "5432/tcp") {
        throw "timescaledb must expose only its PostgreSQL port"
    }
    $bindings = @($portProperties[0].Value)
    if (
        $bindings.Count -ne 1 -or
        $bindings[0].HostIp -cne "127.0.0.1" -or
        $bindings[0].HostPort -cne "5432"
    ) {
        throw "timescaledb port 5432 must be bound exactly once to 127.0.0.1"
    }
}

function Assert-NoConflictingActors {
    param([string[]]$RunningServices)

    $conflictingServices = @(
        $RunningServices | Where-Object { $_ -in @("api", "worker", "beat", "snapshot-builder") }
    )
    if ($conflictingServices.Count -ne 0) {
        throw "stop api, worker, beat, and snapshot-builder before a mutating acquisition command"
    }
    $runningContainerNames = @(docker @script:dockerArgs ps --format "{{.Names}}")
    if ($LASTEXITCODE -ne 0) {
        throw "docker actor-exclusion check failed"
    }
    if (
        @(
            $runningContainerNames |
                Where-Object {
                    $_ -in @(
                        "stockapi-api",
                        "stockapi-worker",
                        "stockapi-beat",
                        "stockapi-snapshot-builder"
                    )
                }
        ).Count -ne 0
    ) {
        throw "stop every persistent stock-api actor before a mutating acquisition command"
    }
    $workerProcessNamePattern = "^(?:py|python(?:w|[0-9]+(?:\.[0-9]+)?)?|celery|uvicorn)(?:\.exe)?$"
    $nativeWorkers = @(
        Get-CimInstance Win32_Process |
            Where-Object {
                $_.ProcessId -ne $PID -and
                $_.Name -match $workerProcessNamePattern -and
                $_.CommandLine -match "(?i)\b(?:celery|uvicorn)\b"
            }
    )
    if ($nativeWorkers.Count -ne 0) {
        throw "stop native Celery and uvicorn processes before a mutating acquisition command"
    }
}

function Assert-ExpectedMigrationHead {
    $query = 'psql --no-psqlrc --no-password --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname stockapi_test --tuples-only --no-align --command "SELECT current_database() || ''|'' || version_num FROM alembic_version"'
    $headOutput = @(docker @script:dockerArgs exec stockapi-timescaledb sh -ceu $query)
    if (
        $LASTEXITCODE -ne 0 -or
        $headOutput.Count -ne 1 -or
        ([string]$headOutput[0]).Trim() -cne "stockapi_test|$script:expectedMigrationHead"
    ) {
        throw "stockapi_test must be at migration head $script:expectedMigrationHead"
    }
}

function Assert-NonReparseDirectory {
    param(
        [string]$Path,
        [string]$Description
    )

    if (-not (Test-Path -LiteralPath $Path -PathType Container)) {
        throw "$Description must be an existing directory"
    }
    $item = Get-Item -LiteralPath $Path -Force
    if (
        -not $item.PSIsContainer -or
        ($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint)
    ) {
        throw "$Description must be a real non-reparse directory"
    }
    return [System.IO.Path]::GetFullPath($item.FullName)
}

function Assert-PrimaryGitState {
    param([string]$ExpectedRevision)

    $root = ([string](git rev-parse --show-toplevel)).Trim()
    $rootExitCode = $LASTEXITCODE
    $branch = ([string](git branch --show-current)).Trim()
    $branchExitCode = $LASTEXITCODE
    $revision = ([string](git rev-parse HEAD)).Trim()
    $revisionExitCode = $LASTEXITCODE
    $status = @(git status --porcelain=v1 --untracked-files=all --ignore-submodules=none)
    $statusExitCode = $LASTEXITCODE
    if (
        $rootExitCode -ne 0 -or
        $branchExitCode -ne 0 -or
        $revisionExitCode -ne 0 -or
        $statusExitCode -ne 0 -or
        [string]::IsNullOrWhiteSpace($root) -or
        [System.IO.Path]::GetFullPath($root) -cne [System.IO.Path]::GetFullPath($PSScriptRoot) -or
        $branch -cne "main" -or
        $revision -cne $ExpectedRevision -or
        $status.Count -ne 0
    ) {
        throw "the acquisition wrapper requires the reviewed clean main revision"
    }
}

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
    if (-not (Test-Path -LiteralPath ".env" -PathType Leaf)) {
        throw ".env is required"
    }
    foreach ($name in $scopedVariableNames) {
        Remove-Item "Env:$name" -ErrorAction SilentlyContinue
    }
    foreach ($name in @(
        "COMPOSE_FILE",
        "COMPOSE_PROFILES",
        "COMPOSE_PROJECT_NAME",
        "COMPOSE_ENV_FILES",
        "COMPOSE_DISABLE_ENV_FILE",
        "DOCKER_CONTEXT",
        "DOCKER_HOST",
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_COMMON_DIR",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES"
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
    $dataDirectoryItem = Get-Item -LiteralPath (Resolve-Path -LiteralPath "data").Path
    if (
        -not $dataDirectoryItem.PSIsContainer -or
        ($dataDirectoryItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint)
    ) {
        throw "the canonical primary data directory must not be a link"
    }
    $acquisitionLedgerPath = [System.IO.Path]::GetFullPath(
        (Join-Path $dataDirectoryItem.FullName "vendor_acquisition_attempts.jsonl")
    )
    $legacyLedgerPath = [System.IO.Path]::GetFullPath(
        (Join-Path $dataDirectoryItem.FullName "vendor_backfill_attempts.jsonl")
    )
    foreach ($ledgerPath in @($acquisitionLedgerPath, $legacyLedgerPath)) {
        if (Test-Path -LiteralPath $ledgerPath) {
            $ledgerItem = Get-Item -LiteralPath $ledgerPath
            if (
                $ledgerItem.PSIsContainer -or
                ($ledgerItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint)
            ) {
                throw "an operator ledger path is not a canonical regular file"
            }
        }
    }
    $composeArgs = @(
        "--env-file", $envFile,
        "--file", $composeFile,
        "--project-directory", $PSScriptRoot,
        "--project-name", "stock-api"
    )
    $runningServices = @(Get-StockApiRunningServices)
    Assert-TimescaleContainer -RunningServices $runningServices
    if ($Mode -ne "plan") {
        Assert-NoConflictingActors -RunningServices $runningServices
    }

    # Prove the primary source tree before executing even the read-only planner.
    # The planner loads .env, so a dirty replacement module must never run first.
    $primaryRevision = ([string](git rev-parse HEAD)).Trim()
    if ($LASTEXITCODE -ne 0 -or $primaryRevision -cnotmatch "^[0-9a-f]{40}$") {
        throw "could not identify the primary source revision"
    }
    Assert-PrimaryGitState -ExpectedRevision $primaryRevision

    if ($Mode -eq "plan") {
        foreach ($name in @(
            "PlanId", "MaxCalls", "SplitCalls", "DividendCalls", "OpenCloseCalls",
            "CampaignId", "CampaignBudgetDelta", "Authorization", "AuthorizationId"
        )) {
            if ($PSBoundParameters.ContainsKey($name)) {
                throw "plan mode accepts only -Mode plan and -End"
            }
        }
        uv run --no-env-file --frozen --project $PSScriptRoot python -m scripts.vendor_acquisition plan `
            --end $End `
            --ledger-path $acquisitionLedgerPath `
            --legacy-ledger-path $legacyLedgerPath
        $commandExitCode = $LASTEXITCODE
    }
    elseif ($Mode -eq "repair") {
        foreach ($name in @(
            "MaxCalls", "SplitCalls", "DividendCalls", "OpenCloseCalls",
            "CampaignId", "CampaignBudgetDelta", "Authorization", "AuthorizationId"
        )) {
            if ($PSBoundParameters.ContainsKey($name)) {
                throw "repair mode requires only -PlanId and never authorizes vendor calls"
            }
        }
        if (-not $PSBoundParameters.ContainsKey("PlanId")) {
            throw "repair mode requires only -PlanId and never authorizes vendor calls"
        }
        uv run --no-env-file --frozen --project $PSScriptRoot python -m scripts.vendor_acquisition repair `
            --end $End `
            --plan-id $PlanId `
            --ledger-path $acquisitionLedgerPath `
            --legacy-ledger-path $legacyLedgerPath
        $commandExitCode = $LASTEXITCODE
    }
    else {
        foreach ($name in @(
            "PlanId", "MaxCalls", "SplitCalls", "DividendCalls", "OpenCloseCalls",
            "CampaignId", "CampaignBudgetDelta", "Authorization", "AuthorizationId"
        )) {
            if (-not $PSBoundParameters.ContainsKey($name)) {
                throw "execute mode requires the plan, campaign, exact typed allocation, authorization, and ID"
            }
        }
        if ($MaxCalls -lt 1) {
            throw "execute mode requires a positive exact current-plan call count"
        }
        if ($MaxCalls -ne ($SplitCalls + $DividendCalls + $OpenCloseCalls)) {
            throw "MaxCalls must equal SplitCalls + DividendCalls + OpenCloseCalls"
        }

        $reviewedPlanOutput = @(
            uv run --no-env-file --frozen --project $PSScriptRoot python -m scripts.vendor_acquisition plan `
                --end $End `
                --ledger-path $acquisitionLedgerPath `
                --legacy-ledger-path $legacyLedgerPath
        )
        if ($LASTEXITCODE -ne 0 -or $reviewedPlanOutput.Count -ne 1) {
            throw "could not revalidate exactly one acquisition plan before execution"
        }
        try {
            $reviewedPlan = $reviewedPlanOutput[0] | ConvertFrom-Json
        }
        catch {
            throw "the acquisition plan revalidation output is malformed"
        }
        $requiredPlanFields = @(
            "status", "plan_id", "tool_revision", "authorization", "symbol", "window_end",
            "required_outbound_attempts", "call_allocation", "campaign_id",
            "campaign_ledger_sha256", "campaign_ledger_record_count",
            "global_ledger_sha256", "global_ledger_record_count",
            "campaign_base_calls", "campaign_authorized_calls",
            "campaign_attempts_reserved", "campaign_remaining_authorized_calls",
            "campaign_required_budget_delta", "campaign_recovery_calls_authorized",
            "campaign_recovery_calls_remaining", "campaign_hard_max_authorized_calls",
            "max_recovery_calls"
        )
        $actualPlanFields = @($reviewedPlan.PSObject.Properties.Name)
        foreach ($name in $requiredPlanFields) {
            if ($actualPlanFields -cnotcontains $name) {
                throw "the acquisition plan is missing a required campaign field"
            }
        }
        $allocationFields = @($reviewedPlan.call_allocation.PSObject.Properties.Name)
        if (
            $allocationFields.Count -ne 3 -or
            $allocationFields -cnotcontains "split_page" -or
            $allocationFields -cnotcontains "dividend_page" -or
            $allocationFields -cnotcontains "open_close" -or
            $reviewedPlan.call_allocation.split_page -isnot [int] -or
            $reviewedPlan.call_allocation.dividend_page -isnot [int] -or
            $reviewedPlan.call_allocation.open_close -isnot [int]
        ) {
            throw "the acquisition plan typed allocation is malformed"
        }
        foreach ($name in @(
            "required_outbound_attempts", "campaign_authorized_calls",
            "campaign_ledger_record_count",
            "campaign_attempts_reserved", "campaign_remaining_authorized_calls",
            "campaign_required_budget_delta", "campaign_recovery_calls_authorized",
            "campaign_recovery_calls_remaining", "campaign_hard_max_authorized_calls",
            "max_recovery_calls"
        )) {
            if ($reviewedPlan.$name -isnot [int] -or $reviewedPlan.$name -lt 0) {
                throw "the acquisition plan campaign counters are malformed"
            }
        }
        if (
            ($reviewedPlan.global_ledger_record_count -isnot [int] -and
                $reviewedPlan.global_ledger_record_count -isnot [long]) -or
            $reviewedPlan.global_ledger_record_count -lt 0
        ) {
            throw "the acquisition plan global ledger counter is malformed"
        }
        if ($null -ne $reviewedPlan.campaign_base_calls -and (
            $reviewedPlan.campaign_base_calls -isnot [int] -or
            $reviewedPlan.campaign_base_calls -lt 1 -or
            $reviewedPlan.campaign_base_calls -gt 259
        )) {
            throw "the acquisition plan campaign base is malformed"
        }
        $expectedCampaignHardMax = if ($null -eq $reviewedPlan.campaign_base_calls) {
            $MaxCalls + $maxRecoveryCalls
        }
        else {
            $reviewedPlan.campaign_base_calls + $maxRecoveryCalls
        }
        if (
            $reviewedPlan.status -cne "ready" -or
            $reviewedPlan.plan_id -cne $PlanId -or
            $reviewedPlan.tool_revision -cnotmatch "^[0-9a-f]{40}$" -or
            $reviewedPlan.authorization -cne "stockapi-msft-acquisition-only" -or
            $reviewedPlan.symbol -cne "MSFT" -or
            $reviewedPlan.window_end -cne $End -or
            $reviewedPlan.required_outbound_attempts -ne $MaxCalls -or
            $reviewedPlan.call_allocation.split_page -ne $SplitCalls -or
            $reviewedPlan.call_allocation.dividend_page -ne $DividendCalls -or
            $reviewedPlan.call_allocation.open_close -ne $OpenCloseCalls -or
            $reviewedPlan.campaign_id -cne $CampaignId -or
            $reviewedPlan.campaign_ledger_sha256 -cnotmatch "^sha256:[0-9a-f]{64}$" -or
            $reviewedPlan.global_ledger_sha256 -cnotmatch "^sha256:[0-9a-f]{64}$" -or
            $reviewedPlan.campaign_ledger_record_count -gt
                $reviewedPlan.global_ledger_record_count -or
            ($null -eq $reviewedPlan.campaign_base_calls -and
                $reviewedPlan.campaign_ledger_record_count -ne 0) -or
            ($null -ne $reviewedPlan.campaign_base_calls -and
                $reviewedPlan.campaign_ledger_record_count -lt 1) -or
            $reviewedPlan.campaign_required_budget_delta -ne $CampaignBudgetDelta -or
            $reviewedPlan.max_recovery_calls -ne $maxRecoveryCalls -or
            $reviewedPlan.campaign_hard_max_authorized_calls -ne $expectedCampaignHardMax -or
            $reviewedPlan.campaign_hard_max_authorized_calls -gt $maxCampaignCalls -or
            $reviewedPlan.campaign_authorized_calls -gt $reviewedPlan.campaign_hard_max_authorized_calls -or
            $reviewedPlan.campaign_attempts_reserved -gt $reviewedPlan.campaign_authorized_calls -or
            $reviewedPlan.campaign_remaining_authorized_calls -ne (
                $reviewedPlan.campaign_authorized_calls - $reviewedPlan.campaign_attempts_reserved
            ) -or
            $reviewedPlan.campaign_recovery_calls_authorized -gt $maxRecoveryCalls -or
            $reviewedPlan.campaign_recovery_calls_remaining -ne (
                $maxRecoveryCalls - $reviewedPlan.campaign_recovery_calls_authorized
            )
        ) {
            throw "the fresh acquisition plan does not exactly match the reviewed authorization"
        }

        Assert-PrimaryGitState -ExpectedRevision $reviewedPlan.tool_revision
        $worktreeRoot = Join-Path ([System.IO.Path]::GetTempPath()) "stockapi-acquisition-worktrees"
        New-Item -ItemType Directory -Force -Path $worktreeRoot | Out-Null
        $resolvedWorktreeRoot = Assert-NonReparseDirectory `
            -Path $worktreeRoot `
            -Description "temporary acquisition worktree root"
        $worktreeRoot = $resolvedWorktreeRoot
        $buildContext = Join-Path $resolvedWorktreeRoot ([guid]::NewGuid().ToString("N"))
        $safeBuildContext = [System.IO.Path]::GetFullPath($buildContext)
        $safeWorktreePrefix = $resolvedWorktreeRoot.TrimEnd(
            [System.IO.Path]::DirectorySeparatorChar
        )
        $safeWorktreePrefix += [System.IO.Path]::DirectorySeparatorChar
        if (
            -not $safeBuildContext.StartsWith(
                $safeWorktreePrefix,
                [StringComparison]::OrdinalIgnoreCase
            ) -or
            (Test-Path -LiteralPath $safeBuildContext)
        ) {
            throw "temporary acquisition worktree escaped its safe root"
        }
        git worktree add --detach $safeBuildContext $reviewedPlan.tool_revision
        if ($LASTEXITCODE -ne 0) {
            throw "could not create the detached reviewed acquisition worktree"
        }
        $buildContextRegistered = $true
        $safeBuildContext = Assert-NonReparseDirectory `
            -Path $safeBuildContext `
            -Description "temporary acquisition worktree"
        $detachedRevision = ([string](git -C $safeBuildContext rev-parse HEAD)).Trim()
        $detachedStatus = @(
            git -C $safeBuildContext status `
                --porcelain=v1 --untracked-files=all --ignore-submodules=none
        )
        if (
            $LASTEXITCODE -ne 0 -or
            $detachedRevision -cne $reviewedPlan.tool_revision -or
            $detachedStatus.Count -ne 0
        ) {
            throw "the detached acquisition worktree is not the reviewed clean revision"
        }

        $runningServices = @(Get-StockApiRunningServices)
        Assert-TimescaleContainer -RunningServices $runningServices
        Assert-NoConflictingActors -RunningServices $runningServices
        Assert-ExpectedMigrationHead
        Assert-PrimaryGitState -ExpectedRevision $reviewedPlan.tool_revision
        $safeBuildContext = Assert-NonReparseDirectory `
            -Path $safeBuildContext `
            -Description "temporary acquisition worktree"
        $detachedFinalRevision = ([string](git -C $safeBuildContext rev-parse HEAD)).Trim()
        $detachedFinalStatus = @(
            git -C $safeBuildContext status `
                --porcelain=v1 --untracked-files=all --ignore-submodules=none
        )
        if (
            $LASTEXITCODE -ne 0 -or
            $detachedFinalRevision -cne $reviewedPlan.tool_revision -or
            $detachedFinalStatus.Count -ne 0
        ) {
            throw "the detached acquisition code changed before execution"
        }
        $env:PYTHONPATH = $safeBuildContext
        uv run --no-env-file --frozen --project $PSScriptRoot python -P -m scripts.vendor_acquisition execute `
            --end $End `
            --plan-id $PlanId `
            --max-calls $MaxCalls `
            --split-calls $SplitCalls `
            --dividend-calls $DividendCalls `
            --open-close-calls $OpenCloseCalls `
            --campaign-id $CampaignId `
            --campaign-budget-delta $CampaignBudgetDelta `
            --authorization $Authorization `
            --authorization-id $AuthorizationId `
            --ledger-path $acquisitionLedgerPath `
            --legacy-ledger-path $legacyLedgerPath
        $commandExitCode = $LASTEXITCODE
    }
}
finally {
    foreach ($name in $scopedVariableNames) {
        $priorValue = $priorScopedVariables[$name]
        if ($null -eq $priorValue) {
            Remove-Item "Env:$name" -ErrorAction SilentlyContinue
        }
        else {
            [Environment]::SetEnvironmentVariable($name, $priorValue, "Process")
        }
    }
    if ($buildContextRegistered -and $null -ne $buildContext -and $null -ne $worktreeRoot) {
        try {
            $safeWorktreeRoot = Assert-NonReparseDirectory `
                -Path $worktreeRoot `
                -Description "temporary acquisition worktree root"
            $safeBuildContext = Assert-NonReparseDirectory `
                -Path $buildContext `
                -Description "temporary acquisition worktree"
            $safeWorktreePrefix = $safeWorktreeRoot.TrimEnd(
                [System.IO.Path]::DirectorySeparatorChar
            ) + [System.IO.Path]::DirectorySeparatorChar
            if (
                -not $safeBuildContext.StartsWith(
                    $safeWorktreePrefix,
                    [StringComparison]::OrdinalIgnoreCase
                )
            ) {
                throw "temporary acquisition worktree cleanup escaped its safe root"
            }
            git worktree remove --force $safeBuildContext 2>$null | Out-Null
            if ($LASTEXITCODE -ne 0) {
                Write-Warning "temporary acquisition worktree cleanup failed"
            }
        }
        catch {
            Write-Warning "temporary acquisition worktree cleanup refused: $($_.Exception.Message)"
        }
    }
    if ($mutexHeld) {
        $mutex.ReleaseMutex()
    }
    $mutex.Dispose()
    Pop-Location
}

if ($commandExitCode -ne 0) {
    exit $commandExitCode
}
