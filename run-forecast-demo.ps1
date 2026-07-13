param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("plan", "execute")]
    [string]$Mode,

    [Parameter(Mandatory = $true)]
    [ValidatePattern("^\d{4}-\d{2}-\d{2}$")]
    [string]$End,

    [ValidatePattern("^sha256:[0-9a-f]{64}$")]
    [string]$PlanId,

    [ValidateSet("stockapi-msft-seal-serve-only")]
    [string]$Authorization
)

# One-command, no-vendor seal-and-serve proof. API_KEYS and database passwords
# remain in ignored .env; no secret is accepted on the command line.
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$mutex = [System.Threading.Mutex]::new($false, "Global\StockApiMutatingOperator")
$mutexHeld = $false
$commandExitCode = 0
$buildContext = $null
$buildContextRegistered = $false
$vendorVariableNames = @(
    "POLYGON_API_KEY",
    "FMP_API_KEY",
    "FINNHUB_API_KEY",
    "NASDAQ_DATA_LINK_API_KEY",
    "ALPACA_API_KEY",
    "ALPACA_API_SECRET",
    "DATABENTO_API_KEY"
)
$attestationVariableNames = @(
    "STOCKAPI_FORECAST_DEMO_REVISION",
    "STOCKAPI_FORECAST_DEMO_API_IMAGE_ID",
    "STOCKAPI_FORECAST_DEMO_BUILDER_IMAGE_ID",
    "STOCKAPI_FORECAST_DEMO_API_CONTAINER_ID",
    "STOCKAPI_API_IMAGE",
    "STOCKAPI_SNAPSHOT_BUILDER_IMAGE"
)
$scopedVariableNames = @($vendorVariableNames) + @($attestationVariableNames)
$priorScopedVariables = @{}
foreach ($name in $scopedVariableNames) {
    $priorScopedVariables[$name] = [Environment]::GetEnvironmentVariable($name, "Process")
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
        throw "another forecast-demo wrapper is already running"
    }
    if (-not (Test-Path -LiteralPath ".env")) {
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
            throw "$name must be unset for the local forecast proof"
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

    if ($Mode -eq "plan") {
        if ($PlanId -or $Authorization) {
            throw "plan mode accepts only -Mode plan and -End"
        }
        uv run python -m scripts.forecast_demo plan --end $End
        $commandExitCode = $LASTEXITCODE
    }
    else {
        if (-not $PlanId -or -not $Authorization) {
            throw "execute mode requires -PlanId and -Authorization"
        }
        $conflictingServices = @(
            $runningServices | Where-Object { $_ -in @("worker", "beat", "snapshot-builder") }
        )
        if ($conflictingServices.Count -ne 0) {
            throw "stop worker, beat, and persistent snapshot-builder before the proof"
        }
        $pythonProcessNamePattern = "^(?:py|python(?:w|[0-9]+(?:\.[0-9]+)?)?|celery|uvicorn)(?:\.exe)?$"
        $nativeWorkers = @(
            Get-CimInstance Win32_Process |
                Where-Object {
                    $_.ProcessId -ne $PID -and
                    $_.Name -match $pythonProcessNamePattern -and
                    $_.CommandLine -match "(?i)\b(?:celery|uvicorn)\b"
                }
        )
        if ($nativeWorkers.Count -ne 0) {
            throw "stop native Celery and uvicorn processes before the proof"
        }
        $gitStatus = @(git status --porcelain=v1 --untracked-files=all --ignore-submodules=none)
        if ($LASTEXITCODE -ne 0 -or $gitStatus.Count -ne 0) {
            throw "execute requires a clean reviewed Git worktree"
        }
        $reviewedPlanOutput = @(
            uv run python -m scripts.forecast_demo plan --end $End
        )
        if ($LASTEXITCODE -ne 0 -or $reviewedPlanOutput.Count -ne 1) {
            throw "could not revalidate the authorized plan before service changes"
        }
        try {
            $reviewedPlan = $reviewedPlanOutput[0] | ConvertFrom-Json
        }
        catch {
            throw "the plan revalidation output is malformed"
        }
        if ($reviewedPlan.status -cne "ready" -or $reviewedPlan.plan_id -cne $PlanId) {
            throw "database or configuration no longer matches the authorized plan"
        }

        # Build one shared app image from an exact detached Git worktree. This
        # prevents an edit/restore race from being mislabeled as the reviewed
        # commit while keeping the vendor worker and Beat entirely stopped.
        $tempRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
        $buildContext = [IO.Path]::GetFullPath((
            Join-Path $tempRoot ("stockapi-forecast-build-" + [guid]::NewGuid().ToString("N"))
        ))
        if (
            -not $buildContext.StartsWith($tempRoot, [StringComparison]::OrdinalIgnoreCase) -or
            (Split-Path -Leaf $buildContext) -cnotmatch "^stockapi-forecast-build-[0-9a-f]{32}$"
        ) {
            throw "refusing an unsafe temporary Git build context"
        }
        git worktree add --detach $buildContext $reviewedPlan.tool_revision
        if ($LASTEXITCODE -ne 0) {
            throw "could not materialize the reviewed Git build context"
        }
        $buildContextRegistered = $true
        docker @dockerArgs build `
            --pull=false `
            --build-arg "STOCKAPI_BUILD_REVISION=$($reviewedPlan.tool_revision)" `
            --tag stock-api-api `
            --tag stock-api-snapshot-builder `
            $buildContext
        if ($LASTEXITCODE -ne 0) {
            throw "forecast-demo image build failed"
        }
        git worktree remove --force $buildContext
        if ($LASTEXITCODE -ne 0) {
            throw "could not remove the temporary Git build context"
        }
        $buildContextRegistered = $false
        $buildContext = $null
        $apiImageId = ([string](
            docker @dockerArgs image inspect stock-api-api --format "{{.Id}}"
        )).Trim()
        if ($LASTEXITCODE -ne 0) {
            throw "could not resolve the immutable API image"
        }
        $builderImageId = ([string](
            docker @dockerArgs image inspect stock-api-snapshot-builder --format "{{.Id}}"
        )).Trim()
        if ($LASTEXITCODE -ne 0) {
            throw "could not resolve the immutable builder image"
        }
        foreach ($imageId in @($apiImageId, $builderImageId)) {
            if ($imageId -cnotmatch "^sha256:[0-9a-f]{64}$") {
                throw "a forecast-demo image has a malformed immutable ID"
            }
            $imageRevision = ([string](
                docker @dockerArgs image inspect $imageId `
                    --format '{{ index .Config.Labels \"org.opencontainers.image.revision\" }}'
            )).Trim()
            if ($LASTEXITCODE -ne 0 -or $imageRevision -cne $reviewedPlan.tool_revision) {
                throw "a forecast-demo image is not bound to the reviewed revision"
            }
        }
        $postBuildGitStatus = @(
            git status --porcelain=v1 --untracked-files=all --ignore-submodules=none
        )
        $postBuildStatusExitCode = $LASTEXITCODE
        $postBuildRevision = ([string](git rev-parse HEAD)).Trim()
        $postBuildRevisionExitCode = $LASTEXITCODE
        if (
            $postBuildStatusExitCode -ne 0 -or
            $postBuildRevisionExitCode -ne 0 -or
            $postBuildGitStatus.Count -ne 0 -or
            $postBuildRevision -cne $reviewedPlan.tool_revision
        ) {
            throw "the reviewed Git state changed during the image build"
        }
        $env:STOCKAPI_API_IMAGE = $apiImageId
        $env:STOCKAPI_SNAPSHOT_BUILDER_IMAGE = $builderImageId
        docker @dockerArgs compose @composeArgs up -d redis-cache redis-celery
        if ($LASTEXITCODE -ne 0) {
            throw "local Redis dependencies failed to start"
        }
        docker @dockerArgs compose @composeArgs --profile app up `
            -d --no-deps --force-recreate --no-build --pull never api
        if ($LASTEXITCODE -ne 0) {
            throw "local API start failed"
        }
        $runningAfterStart = @(
            docker @dockerArgs compose @composeArgs ps --status running --services
        )
        if ($LASTEXITCODE -ne 0) {
            throw "docker compose post-start check failed"
        }
        if ($runningAfterStart -notcontains "api") {
            throw "the local API is not running"
        }
        if (@($runningAfterStart | Where-Object { $_ -in @("worker", "beat", "snapshot-builder") }).Count -ne 0) {
            throw "an unauthorized persistent worker started"
        }
        $runningApiFacts = ([string](
            docker @dockerArgs inspect stockapi-api `
                --format '{{.Id}}|{{.Image}}|{{.State.Running}}|{{ index .Config.Labels \"com.docker.compose.project\" }}|{{ index .Config.Labels \"com.docker.compose.service\" }}|{{ len .Mounts }}'
        )).Trim().Split("|")
        if (
            $LASTEXITCODE -ne 0 -or
            $runningApiFacts.Count -ne 6 -or
            $runningApiFacts[0] -cnotmatch "^[0-9a-f]{64}$" -or
            $runningApiFacts[1] -cne $apiImageId -or
            $runningApiFacts[2] -cne "true" -or
            $runningApiFacts[3] -cne "stock-api" -or
            $runningApiFacts[4] -cne "api" -or
            $runningApiFacts[5] -cne "0"
        ) {
            throw "the running API escaped the attested immutable scope"
        }
        $env:STOCKAPI_FORECAST_DEMO_REVISION = $reviewedPlan.tool_revision
        $env:STOCKAPI_FORECAST_DEMO_API_IMAGE_ID = $apiImageId
        $env:STOCKAPI_FORECAST_DEMO_BUILDER_IMAGE_ID = $builderImageId
        $env:STOCKAPI_FORECAST_DEMO_API_CONTAINER_ID = $runningApiFacts[0]

        uv run python -m scripts.forecast_demo execute `
            --end $End `
            --plan-id $PlanId `
            --authorization $Authorization
        $commandExitCode = $LASTEXITCODE
    }
}
finally {
    if ($buildContextRegistered -and $null -ne $buildContext) {
        $tempRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
        $safeBuildContext = [IO.Path]::GetFullPath($buildContext)
        if (
            $safeBuildContext.StartsWith($tempRoot, [StringComparison]::OrdinalIgnoreCase) -and
            (Split-Path -Leaf $safeBuildContext) -cmatch "^stockapi-forecast-build-[0-9a-f]{32}$"
        ) {
            git worktree remove --force $safeBuildContext 2>$null | Out-Null
            if ($LASTEXITCODE -ne 0) {
                Write-Warning "temporary forecast-demo Git worktree cleanup failed"
            }
        }
    }
    foreach ($name in $scopedVariableNames) {
        $previous = $priorScopedVariables[$name]
        if ($null -eq $previous) {
            Remove-Item "Env:$name" -ErrorAction SilentlyContinue
        }
        else {
            Set-Item "Env:$name" -Value $previous
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
