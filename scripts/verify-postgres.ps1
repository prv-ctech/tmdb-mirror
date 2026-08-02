[CmdletBinding()]
param(
    [switch]$FoundationMigrated
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$projectName = 'tmdb_rust_foundation_test'
$repoRoot = Split-Path -Parent $PSScriptRoot
$composePath = Join-Path $repoRoot 'deploy/compose.dev.yaml'
$envPath = Join-Path $repoRoot 'deploy/env.example'
$containerName = 'tmdb_rust_foundation_test-postgres-1'
$volumeName = 'tmdb_rust_foundation_test_tmdb_pg18_data'
$internalNetworkName = 'tmdb_rust_foundation_test_tmdb-internal'
$loopbackNetworkName = 'tmdb_rust_foundation_test_tmdb-loopback'
$postgresImage = 'postgres:18-bookworm@sha256:1961f96e6029a02c3812d7cb329a3b03a3ac2bb067058dec17b0f5596aca9296'
$postgresRepoDigest = 'postgres@sha256:1961f96e6029a02c3812d7cb329a3b03a3ac2bb067058dec17b0f5596aca9296'

function Invoke-DockerChecked {
    param(
        [Parameter(Mandatory)][string[]]$Arguments,
        [AllowNull()][string]$InputText
    )

    $previousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        if ($PSBoundParameters.ContainsKey('InputText')) {
            $output = $InputText | & docker @Arguments 2>&1
        }
        else {
            $output = & docker @Arguments 2>&1
        }
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    if ($exitCode -ne 0) {
        $safeOutput = [string]::Join("`n", @($output))
        throw "Docker command failed with exit code $exitCode.`n$safeOutput"
    }

    return [string]::Join("`n", @($output)).Trim()
}

function Get-RequiredEnvironmentValue {
    param([Parameter(Mandatory)][string]$Name)

    foreach ($line in Get-Content -LiteralPath $envPath) {
        $match = [regex]::Match($line, "^\s*$([regex]::Escape($Name))\s*=\s*(.*?)\s*$")
        if ($match.Success) {
            $value = $match.Groups[1].Value
            if ([string]::IsNullOrWhiteSpace($value)) {
                break
            }
            return $value
        }
    }
    throw "Required development environment setting is missing: $Name"
}

function Invoke-PostgresScalar {
    param([Parameter(Mandatory)][string]$Sql)

    $arguments = @(
        'compose', '--env-file', $envPath, '-p', $projectName,
        '-f', $composePath, 'exec', '-T', '-e', "PGPASSWORD=$databasePassword", 'postgres',
        'psql', '-X', '-v', 'ON_ERROR_STOP=1', '-U', $databaseUser,
        '-d', $databaseName, '-Atc', $Sql
    )
    return Invoke-DockerChecked -Arguments $arguments
}

function Assert-Exact {
    param(
        [Parameter(Mandatory)][string]$Name,
        [AllowEmptyString()][Parameter(Mandatory)][string]$Actual,
        [AllowEmptyString()][Parameter(Mandatory)][string]$Expected
    )

    if ($Actual -cne $Expected) {
        throw "${Name}: expected '$Expected', got '$Actual'"
    }
    Write-Output "PASS $Name=$Actual"
}

function Assert-NetworkConfiguration {
    param(
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][string]$LogicalName,
        [Parameter(Mandatory)][bool]$Internal
    )

    $labelsJson = Invoke-DockerChecked -Arguments @('network', 'inspect', '--format', '{{json .Labels}}', $Name)
    $labels = $labelsJson | ConvertFrom-Json
    Assert-Exact "network_${LogicalName}_project" ([string]$labels.'com.docker.compose.project') $projectName
    Assert-Exact "network_${LogicalName}_logical_name" ([string]$labels.'com.docker.compose.network') $LogicalName

    $configuration = Invoke-DockerChecked -Arguments @('network', 'inspect', '--format', '{{.Internal}}|{{.Driver}}', $Name)
    $expectedConfiguration = "$($Internal.ToString().ToLowerInvariant())|bridge"
    Assert-Exact "network_${LogicalName}_configuration" $configuration $expectedConfiguration
}

function Assert-DockerRuntime {
    $services = Invoke-DockerChecked -Arguments @(
        'compose', '--env-file', $envPath, '-p', $projectName,
        '-f', $composePath, 'config', '--services'
    )
    Assert-Exact 'compose_services' $services 'postgres'

    $containerLabelsJson = Invoke-DockerChecked -Arguments @('container', 'inspect', '--format', '{{json .Config.Labels}}', $containerName)
    $containerLabels = $containerLabelsJson | ConvertFrom-Json
    Assert-Exact 'container_project_label' ([string]$containerLabels.'com.docker.compose.project') $projectName
    Assert-Exact 'container_service_label' ([string]$containerLabels.'com.docker.compose.service') 'postgres'
    Assert-Exact 'container_running' (Invoke-DockerChecked -Arguments @('container', 'inspect', '--format', '{{.State.Running}}', $containerName)) 'true'
    Assert-Exact 'compose_health' (Invoke-DockerChecked -Arguments @('container', 'inspect', '--format', '{{.State.Health.Status}}', $containerName)) 'healthy'

    $renderedComposeJson = Invoke-DockerChecked -Arguments @(
        'compose', '--env-file', $envPath, '-p', $projectName,
        '-f', $composePath, 'config', '--format', 'json'
    )
    $renderedHealthcheck = ($renderedComposeJson | ConvertFrom-Json).services.postgres.healthcheck
    $expectedRenderedHealthcheckCommand = 'CMD-SHELL|pg_isready -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -h 127.0.0.1 -t 1'
    Assert-Exact 'rendered_healthcheck_command' `
        (@($renderedHealthcheck.test) -join '|') $expectedRenderedHealthcheckCommand
    Assert-Exact 'rendered_healthcheck_timing' `
        "$($renderedHealthcheck.interval)|$($renderedHealthcheck.timeout)|$($renderedHealthcheck.retries)" `
        '2s|3s|30'

    $liveHealthcheckJson = Invoke-DockerChecked -Arguments @(
        'container', 'inspect', '--format', '{{json .Config.Healthcheck}}', $containerName
    )
    $liveHealthcheck = $liveHealthcheckJson | ConvertFrom-Json
    $expectedLiveHealthcheckCommand = 'CMD-SHELL|pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" -h 127.0.0.1 -t 1'
    Assert-Exact 'live_healthcheck_command' `
        (@($liveHealthcheck.Test) -join '|') $expectedLiveHealthcheckCommand
    Assert-Exact 'live_healthcheck_timing' `
        "$($liveHealthcheck.Interval)|$($liveHealthcheck.Timeout)|$($liveHealthcheck.Retries)" `
        '2000000000|3000000000|30'

    Assert-Exact 'container_image_reference' (Invoke-DockerChecked -Arguments @('container', 'inspect', '--format', '{{.Config.Image}}', $containerName)) $postgresImage
    $pinnedImageId = Invoke-DockerChecked -Arguments @('image', 'inspect', '--format', '{{.Id}}', $postgresImage)
    Assert-Exact 'container_image_id' (Invoke-DockerChecked -Arguments @('container', 'inspect', '--format', '{{.Image}}', $containerName)) $pinnedImageId
    $repoDigestsJson = Invoke-DockerChecked -Arguments @('image', 'inspect', '--format', '{{json .RepoDigests}}', $postgresImage)
    $parsedRepoDigests = $repoDigestsJson | ConvertFrom-Json
    [string[]]$actualRepoDigests = @($parsedRepoDigests)
    Assert-Exact 'image_repo_digests' (($actualRepoDigests | Sort-Object -Unique) -join ',') $postgresRepoDigest

    $volumeLabelsJson = Invoke-DockerChecked -Arguments @('volume', 'inspect', '--format', '{{json .Labels}}', $volumeName)
    $volumeLabels = $volumeLabelsJson | ConvertFrom-Json
    Assert-Exact 'volume_project_label' ([string]$volumeLabels.'com.docker.compose.project') $projectName
    Assert-Exact 'volume_logical_name' ([string]$volumeLabels.'com.docker.compose.volume') 'tmdb_pg18_data'
    Assert-Exact 'volume_driver' (Invoke-DockerChecked -Arguments @('volume', 'inspect', '--format', '{{.Driver}}', $volumeName)) 'local'

    $mountsJson = Invoke-DockerChecked -Arguments @('container', 'inspect', '--format', '{{json .Mounts}}', $containerName)
    $mounts = $mountsJson | ConvertFrom-Json
    $expectedMountDestinations = @('/var/lib/postgresql', '/docker-entrypoint-initdb.d')
    [string[]]$actualMountDestinations = @($mounts | ForEach-Object { [string]$_.Destination })
    if ($mounts.Count -ne $expectedMountDestinations.Count) {
        throw "Container mount count drift detected: expected $($expectedMountDestinations.Count), got $($mounts.Count)."
    }
    Assert-Exact 'container_mount_destinations' `
        (($actualMountDestinations | Sort-Object -Unique) -join ',') `
        (($expectedMountDestinations | Sort-Object -Unique) -join ',')

    $volumeMounts = @($mounts | Where-Object { $_.Destination -ceq '/var/lib/postgresql' })
    if ($volumeMounts.Count -ne 1 -or $volumeMounts[0].Type -cne 'volume' -or
        $volumeMounts[0].Name -cne $volumeName -or -not $volumeMounts[0].RW) {
        throw 'Named PostgreSQL volume mount drift detected.'
    }
    Write-Output "PASS postgres_volume_mount=$volumeName|/var/lib/postgresql|rw=true"

    $initMounts = @($mounts | Where-Object { $_.Destination -ceq '/docker-entrypoint-initdb.d' })
    if ($initMounts.Count -ne 1 -or $initMounts[0].Type -cne 'bind' -or $initMounts[0].RW) {
        throw 'PostgreSQL init directory mount drift detected.'
    }
    Write-Output 'PASS postgres_init_mount=/docker-entrypoint-initdb.d|rw=false'

    if (@($mounts | Where-Object { $_.Destination -like '/run/secrets/*' }).Count -ne 0) {
        throw 'Docker secret mounts are not part of the development fixture.'
    }
    Write-Output 'PASS secret_mounts=none'

    $networksJson = Invoke-DockerChecked -Arguments @('container', 'inspect', '--format', '{{json .NetworkSettings.Networks}}', $containerName)
    $networks = $networksJson | ConvertFrom-Json
    [string[]]$actualNetworkNames = @($networks.PSObject.Properties.Name)
    Assert-Exact 'container_networks' `
        (($actualNetworkNames | Sort-Object -Unique) -join ',') `
        ((@($internalNetworkName, $loopbackNetworkName) | Sort-Object -Unique) -join ',')
    Assert-NetworkConfiguration -Name $internalNetworkName -LogicalName 'tmdb-internal' -Internal $true
    Assert-NetworkConfiguration -Name $loopbackNetworkName -LogicalName 'tmdb-loopback' -Internal $false

    $configuredPortsJson = Invoke-DockerChecked -Arguments @('container', 'inspect', '--format', '{{json .HostConfig.PortBindings}}', $containerName)
    $configuredPorts = $configuredPortsJson | ConvertFrom-Json
    Assert-Exact 'configured_port_keys' (($configuredPorts.PSObject.Properties.Name | Sort-Object -Unique) -join ',') '5432/tcp'
    $configuredPostgresBindings = @($configuredPorts.'5432/tcp')
    if ($configuredPostgresBindings.Count -ne 1 -or
        $configuredPostgresBindings[0].HostIp -cne '127.0.0.1' -or
        $configuredPostgresBindings[0].HostPort -cne '55432') {
        throw 'Configured port publication is not exactly 127.0.0.1:55432->5432/tcp.'
    }
    Write-Output 'PASS configured_port=127.0.0.1:55432->5432/tcp'

    $runtimePortsJson = Invoke-DockerChecked -Arguments @('container', 'inspect', '--format', '{{json .NetworkSettings.Ports}}', $containerName)
    $runtimePorts = $runtimePortsJson | ConvertFrom-Json
    Assert-Exact 'runtime_port_keys' (($runtimePorts.PSObject.Properties.Name | Sort-Object -Unique) -join ',') '5432/tcp'
    $runtimePostgresBindings = @($runtimePorts.'5432/tcp')
    if ($runtimePostgresBindings.Count -ne 1 -or
        $runtimePostgresBindings[0].HostIp -cne '127.0.0.1' -or
        $runtimePostgresBindings[0].HostPort -cne '55432') {
        throw 'Runtime port publication is not exactly 127.0.0.1:55432->5432/tcp.'
    }
    Write-Output 'PASS runtime_port=127.0.0.1:55432->5432/tcp'
}

if (-not (Test-Path -LiteralPath $composePath -PathType Leaf)) {
    throw "PostgreSQL Compose definition is missing: $composePath"
}
if (-not (Test-Path -LiteralPath $envPath -PathType Leaf)) {
    throw "Tracked development environment example is missing: $envPath"
}

$databaseName = Get-RequiredEnvironmentValue -Name 'POSTGRES_DB'
$databaseUser = Get-RequiredEnvironmentValue -Name 'POSTGRES_USER'
$databasePassword = Invoke-DockerChecked -Arguments @(
    'compose', '--env-file', $envPath, '-p', $projectName,
    '-f', $composePath, 'exec', '-T', 'postgres', 'printenv', 'POSTGRES_PASSWORD'
)
if ([string]::IsNullOrWhiteSpace($databasePassword)) {
    throw 'PostgreSQL development password is unavailable.'
}

Assert-DockerRuntime

$version = Invoke-PostgresScalar 'SHOW server_version'
if ($version -notlike '18.4*') {
    throw "Expected PostgreSQL 18.4, got $version"
}
Write-Output "PASS server_version=$version"

Assert-Exact 'data_checksums' (Invoke-PostgresScalar 'SHOW data_checksums') 'on'
Assert-Exact 'server_encoding' (Invoke-PostgresScalar 'SHOW server_encoding') 'UTF8'
Assert-Exact 'TimeZone' (Invoke-PostgresScalar 'SHOW TimeZone') 'UTC'
Assert-Exact 'password_encryption' (Invoke-PostgresScalar 'SHOW password_encryption') 'scram-sha-256'
Assert-Exact 'data_directory' (Invoke-PostgresScalar 'SHOW data_directory') '/var/lib/postgresql/18/docker'
Assert-Exact 'track_io_timing' (Invoke-PostgresScalar 'SHOW track_io_timing') 'on'
Assert-Exact 'shared_preload_libraries' (Invoke-PostgresScalar 'SHOW shared_preload_libraries') 'pg_stat_statements'

$extensionsSql = @"
SELECT string_agg(extname, ',' ORDER BY extname)
FROM pg_extension
WHERE extname IN ('pg_stat_statements', 'pg_trgm', 'unaccent')
"@
Assert-Exact 'extensions' (Invoke-PostgresScalar $extensionsSql) 'pg_stat_statements,pg_trgm,unaccent'

$rolesSql = @"
SELECT string_agg(
    rolname || '|' || rolcanlogin::text || '|' || rolinherit::text || '|' ||
    rolsuper::text || '|' || rolcreatedb::text || '|' || rolcreaterole::text || '|' ||
    rolreplication::text || '|' || rolbypassrls::text || '|' ||
    (rolpassword LIKE 'SCRAM-SHA-256`$%')::text,
    E'\n' ORDER BY rolname)
FROM pg_authid
WHERE rolname IN ('migrator', 'api_reader', 'api_job_submitter', 'ingest_writer', 'image_writer', 'monitor')
"@
$expectedRoles = @(
    'api_job_submitter|true|false|false|false|false|false|false|true'
    'api_reader|true|false|false|false|false|false|false|true'
    'image_writer|true|false|false|false|false|false|false|true'
    'ingest_writer|true|false|false|false|false|false|false|true'
    'migrator|true|false|false|false|false|false|false|true'
    'monitor|true|false|false|false|false|false|false|true'
) -join "`n"
Assert-Exact 'role_attributes_and_scram' (Invoke-PostgresScalar $rolesSql) $expectedRoles
Assert-Exact 'owner_scram' (Invoke-PostgresScalar "SELECT (rolpassword LIKE 'SCRAM-SHA-256`$%')::text FROM pg_authid WHERE rolname = current_user") 'true'
Assert-Exact 'host_auth_methods' (Invoke-PostgresScalar "SELECT string_agg(DISTINCT auth_method, ',' ORDER BY auth_method) FROM pg_hba_file_rules WHERE type IN ('host', 'hostssl', 'hostnossl') AND error IS NULL") 'scram-sha-256'

$grantsSql = @"
SELECT string_agg(
    rolname || '|connect=' || has_database_privilege(rolname, current_database(), 'CONNECT')::text ||
    '|create=' || has_database_privilege(rolname, current_database(), 'CREATE')::text ||
    '|public_create=' || has_schema_privilege(rolname, 'public', 'CREATE')::text,
    E'\n' ORDER BY rolname)
FROM pg_roles
WHERE rolname IN ('migrator', 'api_reader', 'api_job_submitter', 'ingest_writer', 'image_writer', 'monitor')
"@
$expectedGrants = @(
    'api_job_submitter|connect=true|create=false|public_create=false'
    'api_reader|connect=true|create=false|public_create=false'
    'image_writer|connect=true|create=false|public_create=false'
    'ingest_writer|connect=true|create=false|public_create=false'
    'migrator|connect=true|create=true|public_create=false'
    'monitor|connect=true|create=false|public_create=false'
) -join "`n"
Assert-Exact 'role_grants' (Invoke-PostgresScalar $grantsSql) $expectedGrants

$publicConnectSql = @"
SELECT coalesce(bool_or(grantee = 0 AND privilege_type = 'CONNECT'), false)::text
FROM pg_database
CROSS JOIN LATERAL aclexplode(COALESCE(datacl, acldefault('d', datdba)))
WHERE datname = current_database()
"@
Assert-Exact 'public_connect_grant' (Invoke-PostgresScalar $publicConnectSql) 'false'

$databaseAclRegressionSql = @"
WITH database_acl AS (
    SELECT datacl, datdba
    FROM pg_database
    WHERE datname = current_database()
), acl_cases(case_name, case_acl, owner_oid) AS (
    SELECT 'null_default', NULL::aclitem[], datdba FROM database_acl
    UNION ALL
    SELECT 'explicit_revoked', datacl, datdba FROM database_acl
)
SELECT string_agg(
    case_name || '=' || coalesce((
        SELECT bool_or(grantee = 0 AND privilege_type = 'CONNECT')
        FROM aclexplode(COALESCE(case_acl, acldefault('d', owner_oid)))
    ), false)::text,
    ',' ORDER BY case_name
)
FROM acl_cases
"@
Assert-Exact 'database_acl_semantics' (Invoke-PostgresScalar $databaseAclRegressionSql) 'explicit_revoked=false,null_default=true'

$applicationSchemasSql = @"
SELECT coalesce(string_agg(nspname, ',' ORDER BY nspname), '')
FROM pg_namespace
WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'public')
  AND nspname NOT LIKE 'pg_toast%'
  AND nspname NOT LIKE 'pg_temp_%'
"@
$expectedApplicationSchemas = if ($FoundationMigrated) {
    'assets,auth,catalog,ops,search,source'
}
else {
    ''
}
Assert-Exact 'application_schemas' (Invoke-PostgresScalar $applicationSchemasSql) $expectedApplicationSchemas

$tcpClient = [System.Net.Sockets.TcpClient]::new()
$asyncResult = $null
try {
    $asyncResult = $tcpClient.BeginConnect('127.0.0.1', 55432, $null, $null)
    if (-not $asyncResult.AsyncWaitHandle.WaitOne(3000)) {
        throw 'Timed out connecting to 127.0.0.1:55432.'
    }
    $tcpClient.EndConnect($asyncResult)
}
finally {
    if ($null -ne $asyncResult) {
        $asyncResult.AsyncWaitHandle.Close()
    }
    $tcpClient.Dispose()
}
Write-Output 'PASS host_tcp_connect=127.0.0.1:55432'

Write-Output 'PostgreSQL 18 development cluster verification passed.'
