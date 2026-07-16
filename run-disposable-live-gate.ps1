# One-shot destructive live-DB gate against a newly created disposable
# TimescaleDB container. This runner never reads the repository's persistent
# PostgreSQL credentials or data directory and never performs vendor calls.
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$script:pinnedTimescaleImage = "timescale/timescaledb:2.28.2-pg17@sha256:909e2c7577c074517c8936b6d8799294b33f25fb5bad978a2199250665f1ce1a"
$script:dockerArgs = @("--context", "desktop-linux")

$mutex = [System.Threading.Mutex]::new($false, "Global\StockApiMutatingOperator")
$mutexHeld = $false
$locationPushed = $false
$dockerRunAttempted = $false
$containerName = ""
$containerId = ""
$volumeName = ""
$persistentSnapshotBefore = $null
$operationError = $null
$cleanupError = $null

$scopedEnvironmentNames = @(
    "TEST_DATABASE_URL",
    "TEST_RUNTIME_DATABASE_URL",
    "TEST_SNAPSHOT_BUILDER_DATABASE_URL",
    "TEST_ALLOW_DESTRUCTIVE_DATABASE_RESET",
    "TEST_DISPOSABLE_DATABASE_HOST_PORT",
    "TEST_DISPOSABLE_DATABASE_NONCE",
    "DATABASE_URL",
    "MIGRATION_DATABASE_URL",
    "APP_ENV",
    "AUTOMATION_ENABLED",
    "POLYGON_TOTAL_CALL_BUDGET",
    "POLYGON_API_KEY",
    "FMP_API_KEY",
    "FINNHUB_API_KEY",
    "NASDAQ_DATA_LINK_API_KEY",
    "ALPACA_API_KEY",
    "ALPACA_API_SECRET",
    "DATABENTO_API_KEY",
    "TEST_RATE_LIMIT_REDIS_URL",
    "TEST_REDIS_URL",
    "UV_OFFLINE"
)
$priorEnvironment = @{}
foreach ($name in $scopedEnvironmentNames) {
    $priorEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, "Process")
}

function New-RandomHex {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateRange(16, 64)]
        [int]$ByteCount
    )

    $bytes = New-Object byte[] $ByteCount
    $generator = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try {
        $generator.GetBytes($bytes)
    }
    finally {
        $generator.Dispose()
    }
    return -join ($bytes | ForEach-Object { $_.ToString("x2") })
}

function Assert-NativeSuccess {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Description
    )

    if ($LASTEXITCODE -ne 0) {
        throw "$Description failed with exit code $LASTEXITCODE"
    }
}

function Write-DockerLogsBestEffort {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Identity,
        [Parameter(Mandatory = $true)]
        [ValidateRange(1, 1000)]
        [int]$Tail
    )

    # PostgreSQL writes normal log records to stderr. Windows PowerShell turns
    # redirected native stderr into ErrorRecords, so diagnostics must not share
    # the caller's terminating-error policy or interfere with cleanup.
    $priorErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        docker @script:dockerArgs logs --tail $Tail $Identity 2>&1 |
            ForEach-Object { Write-Warning ([string]$_) }
        if ($LASTEXITCODE -ne 0) {
            Write-Warning "unable to collect disposable container diagnostics"
        }
    }
    catch {
        Write-Warning "unable to collect disposable container diagnostics"
    }
    finally {
        $ErrorActionPreference = $priorErrorActionPreference
    }
}

function Get-ContainerInspection {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Identity
    )

    $raw = @(docker @script:dockerArgs inspect $Identity)
    Assert-NativeSuccess -Description "Docker container inspection"
    if ($raw.Count -eq 0) {
        throw "Docker returned an empty container inspection"
    }
    $decoded = @((($raw -join "`n") | ConvertFrom-Json))
    if ($decoded.Count -ne 1) {
        throw "Docker returned an ambiguous container inspection"
    }
    return $decoded[0]
}

function Get-LabelValue {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Inspection,
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    if ($null -eq $Inspection.Config.Labels) {
        return ""
    }
    $property = $Inspection.Config.Labels.PSObject.Properties[$Name]
    if ($null -eq $property) {
        return ""
    }
    return [string]$property.Value
}

function Assert-OwnedContainer {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Inspection,
        [Parameter(Mandatory = $true)]
        [string]$ExpectedId,
        [Parameter(Mandatory = $true)]
        [string]$ExpectedNonce
    )

    if ([string]$Inspection.Id -cne $ExpectedId) {
        throw "disposable container identity changed"
    }
    $label = Get-LabelValue -Inspection $Inspection -Name "stockapi.live-gate.nonce"
    if ($label -cne $ExpectedNonce) {
        throw "refusing to act on a container without the exact disposable-gate nonce"
    }
}

function Get-DisposableVolumeName {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Inspection
    )

    $dataMounts = @(
        $Inspection.Mounts |
            Where-Object { [string]$_.Destination -ceq "/var/lib/postgresql/data" }
    )
    if ($dataMounts.Count -ne 1) {
        throw "disposable PostgreSQL data mount is missing or ambiguous"
    }
    $dataMount = $dataMounts[0]
    if ([string]$dataMount.Type -cne "volume" -or -not [bool]$dataMount.RW) {
        throw "PostgreSQL data must use one writable Docker volume"
    }
    $name = [string]$dataMount.Name
    if ([string]::IsNullOrWhiteSpace($name)) {
        throw "Docker did not identify the anonymous PostgreSQL volume"
    }

    $initMounts = @(
        $Inspection.Mounts |
            Where-Object { [string]$_.Destination -ceq "/docker-entrypoint-initdb.d" }
    )
    if ($initMounts.Count -ne 1) {
        throw "database-init bind mount is missing or ambiguous"
    }
    $initMount = $initMounts[0]
    $normalizedSource = ([string]$initMount.Source).Replace("\", "/").TrimEnd("/")
    if (
        [string]$initMount.Type -cne "bind" -or
        [bool]$initMount.RW -or
        -not $normalizedSource.EndsWith(
            "/scripts/db-init",
            [System.StringComparison]::OrdinalIgnoreCase
        )
    ) {
        throw "database-init files must be the repository's read-only bind mount"
    }

    return $name
}

function Get-HostPort5432Snapshot {
    $ids = @(docker @script:dockerArgs ps --all --quiet --no-trunc)
    Assert-NativeSuccess -Description "Docker container listing"
    $records = @()
    foreach ($id in $ids) {
        if ([string]::IsNullOrWhiteSpace([string]$id)) {
            continue
        }
        $inspection = Get-ContainerInspection -Identity ([string]$id).Trim()
        $portProperty = $inspection.NetworkSettings.Ports.PSObject.Properties["5432/tcp"]
        if ($null -eq $portProperty -or $null -eq $portProperty.Value) {
            continue
        }
        $hostBindings = @(
            $portProperty.Value |
                Where-Object { [string]$_.HostPort -ceq "5432" } |
                ForEach-Object {
                    [ordered]@{
                        HostIp = [string]$_.HostIp
                        HostPort = [string]$_.HostPort
                    }
                }
        )
        if ($hostBindings.Count -eq 0) {
            continue
        }
        $mounts = @(
            $inspection.Mounts |
                Sort-Object Destination, Type, Source |
                ForEach-Object {
                    [ordered]@{
                        Type = [string]$_.Type
                        Source = [string]$_.Source
                        Destination = [string]$_.Destination
                        RW = [bool]$_.RW
                    }
                }
        )
        $records += [ordered]@{
            Id = [string]$inspection.Id
            Name = [string]$inspection.Name
            Image = [string]$inspection.Config.Image
            Status = [string]$inspection.State.Status
            StartedAt = [string]$inspection.State.StartedAt
            RestartCount = [int]$inspection.RestartCount
            HostBindings = @($hostBindings | Sort-Object HostIp, HostPort)
            Mounts = $mounts
        }
    }
    $ordered = @($records | Sort-Object Id)
    return [string](ConvertTo-Json -InputObject $ordered -Compress -Depth 8)
}

function Assert-DisposableMarker {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Identity,
        [Parameter(Mandatory = $true)]
        [string]$ExpectedNonce
    )

    $marker = @(
        docker @script:dockerArgs exec $Identity psql `
            --no-psqlrc `
            --username stockapi_owner `
            --dbname stockapi_test `
            --tuples-only `
            --no-align `
            --set ON_ERROR_STOP=1 `
            --command "SELECT nonce FROM public.stockapi_disposable_live_gate_marker WHERE singleton"
    )
    Assert-NativeSuccess -Description "disposable database marker verification"
    $values = @($marker | ForEach-Object { ([string]$_).Trim() } | Where-Object { $_ })
    if ($values.Count -ne 1 -or $values[0] -cne $ExpectedNonce) {
        throw "disposable database marker does not match this invocation"
    }
}

function Set-ScopedEnvironment {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name,
        [AllowEmptyString()]
        [string]$Value
    )

    [Environment]::SetEnvironmentVariable($Name, $Value, "Process")
}

try {
    Push-Location -LiteralPath $PSScriptRoot
    $locationPushed = $true

    try {
        $mutexHeld = $mutex.WaitOne(0)
    }
    catch [System.Threading.AbandonedMutexException] {
        $mutexHeld = $true
    }
    if (-not $mutexHeld) {
        throw "another mutating database or vendor operator is already running"
    }

    foreach ($name in @(
        "ALEMBIC_CONFIG",
        "DOCKER_CONTEXT",
        "DOCKER_HOST",
        "DOCKER_TLS_VERIFY",
        "DOCKER_CERT_PATH",
        "PYTEST_ADDOPTS",
        "PYTEST_PLUGINS"
    )) {
        if (-not [string]::IsNullOrWhiteSpace(
            [Environment]::GetEnvironmentVariable($name, "Process")
        )) {
            throw "$name must be unset for the disposable live-database gate"
        }
    }

    $dockerContext = ([string](docker context show)).Trim()
    Assert-NativeSuccess -Description "Docker context query"
    if ($dockerContext -cne "desktop-linux") {
        throw "Docker must use the local desktop-linux context"
    }
    $dockerEndpoint = ([string](
        docker context inspect "desktop-linux" --format "{{.Endpoints.docker.Host}}"
    )).Trim()
    Assert-NativeSuccess -Description "Docker context inspection"
    if ($dockerEndpoint -cne "npipe:////./pipe/dockerDesktopLinuxEngine") {
        throw "Docker must use the local Docker Desktop Linux endpoint"
    }
    $dockerIdentity = ([string](
        docker @script:dockerArgs info --format "{{.Name}}|{{.OperatingSystem}}"
    )).Trim()
    Assert-NativeSuccess -Description "Docker daemon inspection"
    if ($dockerIdentity -cne "docker-desktop|Docker Desktop") {
        throw "the local Docker Desktop Linux daemon is unavailable"
    }

    $initDirectory = (Resolve-Path -LiteralPath "scripts/db-init").Path
    if (-not (Test-Path -LiteralPath "scripts/db-init/02-runtime-role.sh" -PathType Leaf)) {
        throw "runtime-role bootstrap is missing"
    }

    $nonce = New-RandomHex -ByteCount 16
    $ownerPassword = New-RandomHex -ByteCount 24
    $runtimePassword = New-RandomHex -ByteCount 24
    $builderPassword = New-RandomHex -ByteCount 24
    $credentials = @($ownerPassword, $runtimePassword, $builderPassword)
    if (($credentials | Select-Object -Unique).Count -ne $credentials.Count) {
        throw "generated database credentials collided"
    }

    $containerName = "stockapi-disposable-live-$nonce"
    $existing = @(
        docker @script:dockerArgs ps --all --quiet --no-trunc `
            --filter "name=^/${containerName}$"
    )
    Assert-NativeSuccess -Description "derived container-name availability check"
    if ($existing.Count -ne 0) {
        throw "derived disposable container name already exists"
    }

    $persistentSnapshotBefore = Get-HostPort5432Snapshot

    $dockerRunAttempted = $true
    $containerOutput = @(
        docker @script:dockerArgs run --detach `
            --name $containerName `
            --label "stockapi.live-gate.nonce=$nonce" `
            --platform linux/amd64 `
            --pull always `
            --publish "127.0.0.1::5432" `
            --shm-size 256m `
            --volume /var/lib/postgresql/data `
            --mount "type=bind,source=$initDirectory,target=/docker-entrypoint-initdb.d,readonly" `
            --env POSTGRES_USER=stockapi_owner `
            --env "POSTGRES_PASSWORD=$ownerPassword" `
            --env POSTGRES_DB=stockapi_test `
            --env "POSTGRES_APP_PASSWORD=$runtimePassword" `
            --env "POSTGRES_SNAPSHOT_BUILDER_PASSWORD=$builderPassword" `
            --env TIMESCALEDB_TELEMETRY=off `
            --health-cmd "pg_isready -h 127.0.0.1 -U stockapi_owner -d stockapi_test" `
            --health-interval 3s `
            --health-timeout 5s `
            --health-retries 100 `
            $script:pinnedTimescaleImage
    )
    Assert-NativeSuccess -Description "disposable TimescaleDB container creation"
    $containerIds = @(
        $containerOutput |
            ForEach-Object { ([string]$_).Trim() } |
            Where-Object { $_ }
    )
    if ($containerIds.Count -ne 1 -or $containerIds[0] -cnotmatch "^[0-9a-f]{64}$") {
        throw "Docker did not return one canonical disposable container ID"
    }
    $containerId = $containerIds[0]

    Write-Host "waiting for disposable TimescaleDB health (maximum 5 minutes)..."
    $deadline = (Get-Date).AddMinutes(5)
    do {
        if ((Get-Date) -ge $deadline) {
            Write-DockerLogsBestEffort -Identity $containerId -Tail 100
            throw "timed out waiting for disposable TimescaleDB health"
        }
        Start-Sleep -Seconds 3
        $state = ([string](
            docker @script:dockerArgs inspect `
                --format "{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{else}}missing{{end}}" `
                $containerId
        )).Trim()
        Assert-NativeSuccess -Description "disposable TimescaleDB health inspection"
        if ($state -match "^(exited|dead)\|") {
            throw "disposable TimescaleDB exited during initialization"
        }
        if ($state -match "\|unhealthy$") {
            throw "disposable TimescaleDB became unhealthy"
        }
    } while ($state -cne "running|healthy")

    $inspection = Get-ContainerInspection -Identity $containerId
    Assert-OwnedContainer `
        -Inspection $inspection `
        -ExpectedId $containerId `
        -ExpectedNonce $nonce
    if ([string]$inspection.Config.Image -cne $script:pinnedTimescaleImage) {
        throw "running image does not match the pinned TimescaleDB digest"
    }
    $volumeName = Get-DisposableVolumeName -Inspection $inspection
    $null = @(docker @script:dockerArgs volume inspect $volumeName)
    Assert-NativeSuccess -Description "anonymous PostgreSQL volume inspection"

    $published = @(
        docker @script:dockerArgs port $containerId 5432/tcp |
            ForEach-Object { ([string]$_).Trim() } |
            Where-Object { $_ }
    )
    Assert-NativeSuccess -Description "disposable PostgreSQL port inspection"
    if ($published.Count -ne 1 -or $published[0] -cnotmatch "^127\.0\.0\.1:(?<port>[0-9]+)$") {
        throw "disposable PostgreSQL is not bound to one IPv4 loopback port"
    }
    $hostPort = [int]$Matches.port
    if ($hostPort -lt 1024 -or $hostPort -gt 65535 -or $hostPort -eq 5432) {
        throw "Docker selected a forbidden disposable PostgreSQL host port"
    }
    $portProperty = $inspection.NetworkSettings.Ports.PSObject.Properties["5432/tcp"]
    $portBindings = @($portProperty.Value)
    if (
        $portBindings.Count -ne 1 -or
        [string]$portBindings[0].HostIp -cne "127.0.0.1" -or
        [string]$portBindings[0].HostPort -cne [string]$hostPort
    ) {
        throw "Docker inspection disagrees with the disposable loopback binding"
    }

    docker @script:dockerArgs exec $containerId `
        sh /docker-entrypoint-initdb.d/02-runtime-role.sh | Out-Null
    Assert-NativeSuccess -Description "runtime-role bootstrap replay"
    $bootstrap = @(
        docker @script:dockerArgs exec $containerId psql `
            --no-psqlrc `
            --username stockapi_owner `
            --dbname stockapi_test `
            --tuples-only `
            --no-align `
            --set ON_ERROR_STOP=1 `
            --command "SELECT (EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') AND (SELECT count(*) FROM pg_roles WHERE rolname IN ('stockapi_app', 'stockapi_snapshot_builder') AND rolcanlogin) = 2)::int"
    )
    Assert-NativeSuccess -Description "TimescaleDB and runtime-role verification"
    $bootstrapValues = @(
        $bootstrap | ForEach-Object { ([string]$_).Trim() } | Where-Object { $_ }
    )
    if ($bootstrapValues.Count -ne 1 -or $bootstrapValues[0] -cne "1") {
        throw "TimescaleDB extension or runtime-role bootstrap is missing"
    }

    $markerSql = @"
CREATE TABLE public.stockapi_disposable_live_gate_marker (
    singleton boolean PRIMARY KEY CHECK (singleton),
    nonce varchar(32) NOT NULL CHECK (nonce ~ '^[0-9a-f]{32}$')
);
INSERT INTO public.stockapi_disposable_live_gate_marker (singleton, nonce)
VALUES (true, '$nonce');
REVOKE ALL ON TABLE public.stockapi_disposable_live_gate_marker
FROM PUBLIC, stockapi_app, stockapi_snapshot_builder;
"@
    docker @script:dockerArgs exec $containerId psql `
        --no-psqlrc `
        --username stockapi_owner `
        --dbname stockapi_test `
        --set ON_ERROR_STOP=1 `
        --command $markerSql | Out-Null
    Assert-NativeSuccess -Description "disposable database marker creation"
    Assert-DisposableMarker -Identity $containerId -ExpectedNonce $nonce

    $ownerUrlPassword = [uri]::EscapeDataString($ownerPassword)
    $runtimeUrlPassword = [uri]::EscapeDataString($runtimePassword)
    $builderUrlPassword = [uri]::EscapeDataString($builderPassword)
    $ownerUrl = "postgresql+asyncpg://stockapi_owner:${ownerUrlPassword}@127.0.0.1:${hostPort}/stockapi_test"
    $runtimeUrl = "postgresql+asyncpg://stockapi_app:${runtimeUrlPassword}@127.0.0.1:${hostPort}/stockapi_test"
    $builderUrl = "postgresql+asyncpg://stockapi_snapshot_builder:${builderUrlPassword}@127.0.0.1:${hostPort}/stockapi_test"

    Set-ScopedEnvironment -Name "TEST_DATABASE_URL" -Value $ownerUrl
    Set-ScopedEnvironment -Name "TEST_RUNTIME_DATABASE_URL" -Value $runtimeUrl
    Set-ScopedEnvironment -Name "TEST_SNAPSHOT_BUILDER_DATABASE_URL" -Value $builderUrl
    Set-ScopedEnvironment `
        -Name "TEST_ALLOW_DESTRUCTIVE_DATABASE_RESET" `
        -Value "stockapi-disposable-container-only"
    Set-ScopedEnvironment -Name "TEST_DISPOSABLE_DATABASE_HOST_PORT" -Value ([string]$hostPort)
    Set-ScopedEnvironment -Name "TEST_DISPOSABLE_DATABASE_NONCE" -Value $nonce
    Set-ScopedEnvironment -Name "DATABASE_URL" -Value $runtimeUrl
    Set-ScopedEnvironment -Name "MIGRATION_DATABASE_URL" -Value $ownerUrl
    Set-ScopedEnvironment -Name "APP_ENV" -Value "test"
    Set-ScopedEnvironment -Name "AUTOMATION_ENABLED" -Value "false"
    Set-ScopedEnvironment -Name "POLYGON_TOTAL_CALL_BUDGET" -Value "0"
    Set-ScopedEnvironment -Name "UV_OFFLINE" -Value "1"
    foreach ($name in @(
        "POLYGON_API_KEY",
        "FMP_API_KEY",
        "FINNHUB_API_KEY",
        "NASDAQ_DATA_LINK_API_KEY",
        "ALPACA_API_KEY",
        "ALPACA_API_SECRET",
        "DATABENTO_API_KEY",
        "TEST_RATE_LIMIT_REDIS_URL",
        "TEST_REDIS_URL"
    )) {
        Set-ScopedEnvironment -Name $name -Value ""
    }

    uv run --no-env-file --frozen --no-sync --project $PSScriptRoot pytest `
        -p no:cacheprovider `
        --basetemp "data/pytest-disposable-live-$nonce" `
        tests/integration/test_bars_live_gate.py `
        --tb=short `
        -v
    Assert-NativeSuccess -Description "disposable live integration gate"

    Assert-DisposableMarker -Identity $containerId -ExpectedNonce $nonce
}
catch {
    $operationError = $_
}
finally {
    try {
        try {
            if ($dockerRunAttempted) {
                if ([string]::IsNullOrWhiteSpace($containerId)) {
                    $candidateIds = @(
                        docker @script:dockerArgs ps --all --quiet --no-trunc `
                            --filter "label=stockapi.live-gate.nonce=$nonce"
                    )
                    Assert-NativeSuccess -Description "disposable cleanup candidate lookup"
                    if ($candidateIds.Count -gt 1) {
                        throw "disposable cleanup lookup returned multiple owned containers"
                    }
                    if ($candidateIds.Count -eq 1) {
                        $containerId = ([string]$candidateIds[0]).Trim()
                    }
                }

                if (-not [string]::IsNullOrWhiteSpace($containerId)) {
                    $cleanupInspection = Get-ContainerInspection -Identity $containerId
                    Assert-OwnedContainer `
                        -Inspection $cleanupInspection `
                        -ExpectedId $containerId `
                        -ExpectedNonce $nonce
                    if ([string]::IsNullOrWhiteSpace($volumeName)) {
                        $volumeName = Get-DisposableVolumeName -Inspection $cleanupInspection
                    }
                    if ($null -ne $operationError) {
                        Write-DockerLogsBestEffort -Identity $containerId -Tail 200
                    }
                    docker @script:dockerArgs rm --force --volumes $containerId | Out-Null
                    Assert-NativeSuccess -Description "disposable container and volume cleanup"

                    $remainingContainers = @(
                        docker @script:dockerArgs ps --all --quiet --no-trunc `
                            --filter "id=$containerId"
                    )
                    Assert-NativeSuccess -Description "disposable container removal verification"
                    if ($remainingContainers.Count -ne 0) {
                        throw "disposable container still exists after cleanup"
                    }
                    $remainingVolumes = @(
                        docker @script:dockerArgs volume ls --quiet `
                            --filter "name=$volumeName" |
                            Where-Object { ([string]$_).Trim() -ceq $volumeName }
                    )
                    Assert-NativeSuccess -Description "anonymous volume removal verification"
                    if ($remainingVolumes.Count -ne 0) {
                        throw "anonymous PostgreSQL volume still exists after cleanup"
                    }
                }
            }
        }
        finally {
            if ($null -ne $persistentSnapshotBefore) {
                $persistentSnapshotAfter = Get-HostPort5432Snapshot
                if ($persistentSnapshotAfter -cne $persistentSnapshotBefore) {
                    throw "the persistent host-port-5432 container snapshot changed during the disposable gate"
                }
            }
        }
    }
    catch {
        $cleanupError = $_
    }
    finally {
        try {
            foreach ($name in $scopedEnvironmentNames) {
                [Environment]::SetEnvironmentVariable(
                    $name,
                    $priorEnvironment[$name],
                    "Process"
                )
            }
        }
        finally {
            if ($mutexHeld) {
                $mutex.ReleaseMutex()
            }
            $mutex.Dispose()
            if ($locationPushed) {
                Pop-Location
            }
        }
    }
}

if ($null -ne $operationError -and $null -ne $cleanupError) {
    throw [System.InvalidOperationException]::new(
        "disposable gate failed: $($operationError.Exception.Message); cleanup attestation also failed: $($cleanupError.Exception.Message)",
        $operationError.Exception
    )
}
if ($null -ne $operationError) {
    throw $operationError
}
if ($null -ne $cleanupError) {
    throw $cleanupError
}
