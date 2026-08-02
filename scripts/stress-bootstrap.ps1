[CmdletBinding()]
param(
    [string]$ProjectName = 'tmdb_stress_test',
    [int]$ApiPort = 18080,
    [int]$AdminPort = 18081,
    [int]$ImagePort = 18090,
    [int]$PostgresPort = 55433,
    [string]$TmdbReadToken = $env:TMDB_STRESS_READ_TOKEN,
    [string]$TrawlBaseUrl = $env:TMDB_STRESS_TRAWL_BASE_URL,
    [string]$SecretsFile,
    [switch]$SkipBuild
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $PSScriptRoot
. (Join-Path $repoRoot 'scripts/stress-secrets.ps1')
$composeFile = Join-Path $repoRoot 'deploy/compose.stress.yaml'
$runtimeRoot = Join-Path (Join-Path $repoRoot '.stress-runtime') $ProjectName
$envFile = Join-Path $runtimeRoot 'compose.env'
$metadataFile = Join-Path $runtimeRoot 'metadata.json'
$appImage = 'tmdb-stress-app:local'

if ([string]::IsNullOrWhiteSpace($SecretsFile)) {
    $SecretsFile = Join-Path $repoRoot 'secrets.txt'
}
elseif (-not (Test-Path -LiteralPath $SecretsFile -PathType Leaf)) {
    throw "Local stress secrets file is missing: $SecretsFile"
}
$localSecrets = Read-StressSecrets -Path $SecretsFile
$TmdbReadToken = Resolve-StressSecret `
    -Secrets $localSecrets -Name 'TMDB_STRESS_READ_TOKEN' -ExplicitValue $TmdbReadToken
$TrawlBaseUrl = Resolve-StressSecret `
    -Secrets $localSecrets -Name 'TMDB_STRESS_TRAWL_BASE_URL' -ExplicitValue $TrawlBaseUrl

function Invoke-DockerChecked {
    param([Parameter(Mandatory)][string[]]$Arguments)

    $previous = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $output = @(& docker @Arguments 2>&1)
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previous
    }
    if ($exitCode -ne 0) {
        $text = [string]::Join("`n", $output)
        throw "Docker command failed with exit code $exitCode.`n$text"
    }
    return [string]::Join("`n", $output).Trim()
}

function Write-Utf8NoBom {
    param([Parameter(Mandatory)][string]$Path, [Parameter(Mandatory)][string]$Value)
    $encoding = [System.Text.UTF8Encoding]::new($false)
    [System.IO.File]::WriteAllText($Path, $Value, $encoding)
}

function Assert-PortAvailable {
    param([Parameter(Mandatory)][int]$Port)
    $listener = $null
    try {
        $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, $Port)
        $listener.Start()
    }
    catch {
        throw "Stress-test port 127.0.0.1:$Port is already in use. Choose another port."
    }
    finally {
        if ($null -ne $listener) { $listener.Stop() }
    }
}

function Get-ComposeArguments {
    return @('--env-file', $envFile, '--project-name', $ProjectName, '--file', $composeFile)
}

function Invoke-Compose {
    param([Parameter(Mandatory)][string[]]$Arguments)
    Invoke-DockerChecked -Arguments (@('compose') + (Get-ComposeArguments) + $Arguments)
}

function Wait-ServiceHealthy {
    param(
        [Parameter(Mandatory)][string]$Service,
        [int]$TimeoutSeconds = 180
    )
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        $containerId = $null
        try {
            $containerId = (Invoke-Compose -Arguments @('ps', '-q', $Service)).Trim()
        }
        catch { $containerId = $null }
        if ($containerId) {
            $state = Invoke-DockerChecked -Arguments @('inspect', '--format', '{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{end}}', $containerId)
            if ($state -eq 'running|healthy') { return }
            if ($state -match '^exited\|' -or $state -match '^dead\|') {
                $logs = Invoke-Compose -Arguments @('logs', '--no-color', '--timestamps', $Service)
                throw "Stress service '$Service' stopped before becoming healthy.`n$logs"
            }
        }
        Start-Sleep -Seconds 2
    } while ([DateTime]::UtcNow -lt $deadline)
    $logs = Invoke-Compose -Arguments @('logs', '--no-color', '--timestamps', $Service)
    throw "Timed out waiting for stress service '$Service'.`n$logs"
}

function Wait-Migrations {
    param([int]$TimeoutSeconds = 180)

    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        try {
            $password = (Invoke-Compose -Arguments @('exec', '-T', 'postgres', 'printenv', 'POSTGRES_PASSWORD')).Trim()
            $migrationsExist = (Invoke-Compose -Arguments @(
                'exec', '-T', '-e', "PGPASSWORD=$password", 'postgres', 'psql', '-X', '-At',
                '--username', 'tmdb_owner', '--dbname', 'tmdb', '-c',
                "SELECT to_regclass('ops._sqlx_migrations') IS NOT NULL"
            )).Trim()
            if ($migrationsExist -eq 't') {
                $version = (Invoke-Compose -Arguments @(
                    'exec', '-T', '-e', "PGPASSWORD=$password", 'postgres', 'psql', '-X', '-At',
                    '--username', 'tmdb_owner', '--dbname', 'tmdb', '-c',
                    "SELECT COALESCE(max(version), 0) FROM ops._sqlx_migrations WHERE success"
                )).Trim()
                if ([int]$version -ge 19) { return }
            }
        }
        catch {
            # The worker may still be opening its migration connection.
        }
        Start-Sleep -Seconds 2
    } while ([DateTime]::UtcNow -lt $deadline)
    $logs = Invoke-Compose -Arguments @('logs', '--no-color', '--timestamps', 'worker')
    throw "Timed out waiting for consolidated worker migrations.`n$logs"
}

if (-not (Test-Path -LiteralPath $composeFile -PathType Leaf)) {
    throw "Stress Compose definition is missing: $composeFile"
}
if ($ProjectName -notmatch '^[a-z0-9][a-z0-9_-]{2,48}$') {
    throw 'ProjectName must be 3-49 lowercase letters, digits, underscores, or hyphens.'
}
if ([string]::IsNullOrWhiteSpace($TmdbReadToken) -or $TmdbReadToken.Contains("`r") -or $TmdbReadToken.Contains("`n")) {
    throw 'Provide the TMDB read token through -TmdbReadToken or TMDB_STRESS_READ_TOKEN for this test run.'
}
if (-not [string]::IsNullOrWhiteSpace($TrawlBaseUrl)) {
    if ($TrawlBaseUrl.Contains("`r") -or $TrawlBaseUrl.Contains("`n")) {
        throw 'TMDB_STRESS_TRAWL_BASE_URL must be a single-line URL.'
    }
    $trawlUri = $null
    if (-not [Uri]::TryCreate($TrawlBaseUrl.Trim(), [UriKind]::Absolute, [ref]$trawlUri) -or
        $trawlUri.Scheme -notin @('http', 'https') -or
        [string]::IsNullOrWhiteSpace($trawlUri.Host) -or
        $null -ne $trawlUri.Query -and $trawlUri.Query.Length -gt 0 -or
        $null -ne $trawlUri.UserInfo -and $trawlUri.UserInfo.Length -gt 0) {
        throw 'TMDB_STRESS_TRAWL_BASE_URL must be an http(s) URL without credentials or a query string.'
    }
    $TrawlBaseUrl = $TrawlBaseUrl.TrimEnd('/')
}
foreach ($port in @($ApiPort, $AdminPort, $ImagePort, $PostgresPort)) { Assert-PortAvailable -Port $port }

$null = Invoke-DockerChecked -Arguments @('version', '--format', '{{.Server.Version}}')

try {
    New-Item -ItemType Directory -Path $runtimeRoot -Force | Out-Null
    $envFileForCompose = $envFile.Replace('\', '/')
    $envText = @(
        "TMDB_STRESS_PROJECT=$ProjectName",
        "TMDB_STRESS_ENV_FILE=$envFileForCompose",
        "TMDB_STRESS_APP_IMAGE=$appImage",
        "TMDB_STRESS_API_PORT=$ApiPort",
        "TMDB_STRESS_ADMIN_PORT=$AdminPort",
        "TMDB_STRESS_IMAGE_PORT=$ImagePort",
        "TMDB_STRESS_PG_PORT=$PostgresPort",
        'TMDB_ENVIRONMENT=test',
        'POSTGRES_DB=tmdb',
        'POSTGRES_USER=tmdb_owner',
        'POSTGRES_PASSWORD=tmdb-stress',
        'PGDATA=/var/lib/postgresql/18/docker',
        'POSTGRES_INITDB_ARGS=--data-checksums --encoding=UTF8 --auth-local=scram-sha-256 --auth-host=scram-sha-256',
        'DATABASE_HOST=postgres',
        'DATABASE_PORT=5432',
        'DATABASE_NAME=tmdb',
        'DATABASE_USER=tmdb_owner',
        'TMDB_API_BIND=0.0.0.0:8080',
        'TMDB_ADMIN_BIND=0.0.0.0:8081',
        'TMDB_MEDIA_BIND=0.0.0.0:8090',
        'TMDB_ADMIN_API_KEY=test-admin-key-placeholder-0123456789',
        "TMDB_READ_ACCESS_TOKEN=$($TmdbReadToken.Trim())",
        'TMDB_API_BASE_URL=https://api.themoviedb.org/3',
        "TMDB_MEDIA_BASE_URL=http://127.0.0.1:$ImagePort/media",
        'TMDB_RATE_LIMIT=40',
        'TMDB_MAX_CONNECTIONS=20',
        'TMDB_MAX_ATTEMPTS=4',
        'TMDB_REQUEST_TIMEOUT_SECONDS=30',
        'TMDB_DAILY_EXPORT_MAX_BYTES=536870912',
        'TMDB_WORKER_ID=tmdb-stress-worker',
        'TMDB_IMAGE_WORKER_ID=tmdb-stress-media',
        'TMDB_IMAGE_WORKER_CONCURRENCY=4',
        'TMDB_WORKER_LEASE_SECONDS=60',
        'TMDB_WORKER_HEARTBEAT_SECONDS=15',
        'TMDB_WORKER_IDLE_POLL_MS=100',
        'TMDB_STRESS_PG_MAX_CONNECTIONS=120',
        'TMDB_STRESS_PG_SHARED_BUFFERS=2GB',
        'TMDB_STRESS_PG_EFFECTIVE_CACHE_SIZE=8GB',
        'TMDB_STRESS_PG_WORK_MEM=32MB',
        'TMDB_STRESS_PG_MAINTENANCE_WORK_MEM=512MB',
        'ALLOW_LOCAL_MEDIA=true',
        'TMDB_ENABLE_SCHEDULER=true',
        'TMDB_SCHEDULER_INTERVAL_SECONDS=60',
        'TMDB_ENABLE_DAILY_EXPORT=false'
    )
    if (-not [string]::IsNullOrWhiteSpace($TrawlBaseUrl)) {
        $envText += "TMDB_TRAWL_BASE_URL=$TrawlBaseUrl"
    }
    Write-Utf8NoBom -Path $envFile -Value ([string]::Join("`n", $envText) + "`n")

    $metadata = [ordered]@{
        project = $ProjectName
        compose_file = $composeFile
        runtime_root = $runtimeRoot
        api_url = "http://127.0.0.1:$ApiPort"
        admin_url = "http://127.0.0.1:$AdminPort"
        image_url = "http://127.0.0.1:$ImagePort"
        postgres_host = '127.0.0.1'
        postgres_port = $PostgresPort
        started_at_utc = [DateTime]::UtcNow.ToString('O')
    }
    Write-Utf8NoBom -Path $metadataFile -Value (($metadata | ConvertTo-Json -Depth 4) + "`n")

    if (-not $SkipBuild) {
        Write-Output 'Building the pinned Rust application image...'
        Invoke-DockerChecked -Arguments @('build', '--pull=false', '--file', (Join-Path $repoRoot 'Dockerfile'), '--tag', $appImage, $repoRoot) | Write-Output
    }

    Write-Output "Starting isolated PostgreSQL project '$ProjectName'..."
    Invoke-Compose -Arguments @('up', '-d', '--remove-orphans', 'postgres') | Write-Output
    Wait-ServiceHealthy -Service 'postgres'
    Write-Output 'Starting the consolidated worker so it applies migrations...'
    Invoke-Compose -Arguments @('up', '-d', '--remove-orphans', 'worker') | Write-Output
    Wait-Migrations
    # The runtime image is intentionally non-root. Prepare the disposable
    # disk-backed media volume once so the worker can write permanent assets
    # without adding a storage-init service to the four-container topology.
    $mediaVolume = "$ProjectName`_media"
    Invoke-DockerChecked -Arguments @(
        'volume', 'create', '--label', "com.docker.compose.project=$ProjectName",
        '--label', 'com.docker.compose.volume=tmdb_stress_media', $mediaVolume
    ) | Out-Null
    Invoke-DockerChecked -Arguments @(
        'run', '--rm', '--user', '0:0', '--mount', "type=volume,source=$mediaVolume,target=/media,volume-nocopy=true",
        '--entrypoint', '/usr/bin/chown', $appImage, '-R', '10001:10001', '/media'
    ) | Out-Null
    Invoke-DockerChecked -Arguments @(
        'run', '--rm', '--user', '10001:10001', '--mount', "type=volume,source=$mediaVolume,target=/media,volume-nocopy=true",
        '--entrypoint', '/bin/sh', $appImage, '-c', 'touch /media/.write-test && rm /media/.write-test'
    ) | Out-Null
    Write-Output 'Starting API and media worker...'
    Invoke-Compose -Arguments @('up', '-d', '--remove-orphans', 'api', 'media') | Write-Output
    Wait-ServiceHealthy -Service 'api'
    Wait-ServiceHealthy -Service 'media'
    Write-Output "Stress stack is ready: http://127.0.0.1:$ApiPort"
    Write-Output "Runtime metadata: $metadataFile"
}
finally {
    $TmdbReadToken = $null
    $TrawlBaseUrl = $null
    $localSecrets = $null
    if (Test-Path Env:TMDB_STRESS_READ_TOKEN) { Remove-Item Env:TMDB_STRESS_READ_TOKEN -ErrorAction SilentlyContinue }
}
