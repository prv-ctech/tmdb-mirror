[CmdletBinding()]
param(
    [string]$EnvFile = '',
    [string]$ComposeFile = ''
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

# Resolve defaults after PowerShell has initialized $PSScriptRoot. Parameter
# default expressions are evaluated before that automatic variable is usable
# when the script is invoked from another working directory.
if ([string]::IsNullOrWhiteSpace($EnvFile)) {
    $EnvFile = Join-Path $PSScriptRoot '..\.env'
}
if ([string]::IsNullOrWhiteSpace($ComposeFile)) {
    $ComposeFile = Join-Path $PSScriptRoot '..\deploy\compose.production.yaml'
}

function Resolve-RequiredFile {
    param([Parameter(Mandatory)][string]$Path, [Parameter(Mandatory)][string]$Name)
    $resolved = Resolve-Path -LiteralPath $Path -ErrorAction SilentlyContinue
    if ($null -eq $resolved -or -not (Test-Path -LiteralPath $resolved.Path -PathType Leaf)) {
        throw "$Name is missing: $Path"
    }
    return $resolved.Path
}

$envPath = Resolve-RequiredFile -Path $EnvFile -Name 'production env file'
$composePath = Resolve-RequiredFile -Path $ComposeFile -Name 'production Compose file'

# Validate only behavior settings and fixed container-path contracts. Values
# are never echoed.
$entries = @{}
foreach ($line in Get-Content -LiteralPath $envPath) {
    if ($line -match '^\s*(?<key>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*(?<value>[^#]*?)\s*$') {
        $value = $Matches.value.Trim()
        if ($value.Length -ge 2 -and
            (($value.StartsWith("'") -and $value.EndsWith("'")) -or
             ($value.StartsWith('"') -and $value.EndsWith('"')))) {
            $value = $value.Substring(1, $value.Length - 2)
        }
        $entries[$Matches.key] = $value
    }
}

$requiredKeys = @(
    'TMDB_ENVIRONMENT', 'POSTGRES_DB', 'POSTGRES_USER', 'POSTGRES_PASSWORD',
    'TMDB_READ_ACCESS_TOKEN', 'TMDB_ADMIN_API_KEY', 'ALLOW_LOCAL_MEDIA',
    'TMDB_MEDIA_BASE_URL', 'TZ'
)
foreach ($key in $requiredKeys) {
    if (-not $entries.ContainsKey($key) -or [string]::IsNullOrWhiteSpace($entries[$key])) {
        throw "Required deployment setting is missing: $key"
    }
}

# The four-container deployment has one PostgreSQL identity and a fixed
# in-network endpoint. Reject obsolete aliases before Compose starts so an
# accidental copy from an earlier template cannot be silently ignored.
$unsupportedDatabaseKeys = @(
    'DATABASE_HOST', 'DATABASE_PORT', 'DATABASE_NAME', 'DATABASE_USER', 'DATABASE_PASSWORD',
    'TMDB_DB_HOST', 'TMDB_DB_PORT', 'TMDB_DB_NAME', 'TMDB_DB_USER', 'TMDB_DB_PASSWORD',
    'TMDB_DIRECT_DB_HOST', 'TMDB_DIRECT_DB_PORT', 'TMDB_DIRECT_DB_NAME', 'TMDB_DIRECT_DB_USER', 'TMDB_DIRECT_DB_PASSWORD',
    'TMDB_POOLED_DB_HOST', 'TMDB_POOLED_DB_PORT', 'TMDB_POOLED_DB_NAME', 'TMDB_POOLED_DB_USER', 'TMDB_POOLED_DB_PASSWORD'
)
foreach ($key in $unsupportedDatabaseKeys) {
    if ($entries.ContainsKey($key) -or $entries.ContainsKey("${key}_FILE")) {
        throw "Unsupported database setting: $key. Use POSTGRES_DB, POSTGRES_USER, and POSTGRES_PASSWORD."
    }
}

$unsupportedStorageKeys = @(
    'TMDB_MEDIA_HOST_ROOT', 'TMDB_WORK_HOST_ROOT', 'TMDB_MEDIA_ROOT', 'TMDB_WORK_ROOT'
)
foreach ($key in $unsupportedStorageKeys) {
    if ($entries.ContainsKey($key) -or $entries.ContainsKey("${key}_FILE")) {
        throw "Unsupported filesystem-root setting: $key. Mount the fixed /config or /media container path instead."
    }
}

$composeText = Get-Content -LiteralPath $composePath -Raw
foreach ($target in @('/media', '/config')) {
    if ($composeText -notmatch "target:\s*$([regex]::Escape($target))") {
        throw "Compose template is missing the fixed container mount: $target"
    }
}
foreach ($service in @('postgres:', 'api:', 'worker:', 'media:')) {
    if ($composeText -notmatch "(?m)^\s{2}$([regex]::Escape($service))") {
        throw "Compose template is missing the canonical service: $service"
    }
}
foreach ($legacy in @('pgbouncer:', 'image-server:', 'admin-migrate:', 'storage-init:')) {
    if ($composeText -match "(?m)^\s{2}$([regex]::Escape($legacy))") {
        throw "Legacy extra service remains in the canonical Compose template: $legacy"
    }
}
if ($composeText -match 'pg_isready\s+-U\s+tmdb_owner|pg_isready\s+.*-d\s+tmdb') {
    throw 'PostgreSQL health checks must use POSTGRES_USER and POSTGRES_DB from the shared environment file.'
}
if ($composeText -notmatch '\$\$POSTGRES_USER' -or $composeText -notmatch '\$\$POSTGRES_DB') {
    throw 'PostgreSQL health checks must interpolate POSTGRES_USER and POSTGRES_DB inside the container.'
}
foreach ($setting in @('wal_level=replica', 'archive_mode=on', 'archive_command=pgbackrest --stanza=tmdb archive-push %p')) {
    if ($composeText -notmatch [regex]::Escape($setting)) {
        throw "PostgreSQL PITR setting is missing: $setting"
    }
}
if ($composeText -match '(?m)^\s*-\s*["'']?[^\s"'']*:8081') {
    throw 'The private admin listener must not be published to a host port.'
}
if ($composeText -notmatch 'prv-network:' -or $composeText -notmatch 'name:\s*prv\.network' -or
    $composeText -notmatch 'tmdb-mirror-api') {
    throw 'The API must expose the private admin listener only through the existing prv.network alias.'
}

# The worker and media service must use the bounded root-only preparer, which
# creates only the fixed child paths before it drops to UID/GID 10001. Do not
# silently regress to a manual host chmod/chown requirement or a broadly
# privileged application process.
foreach ($role in @('worker', 'media')) {
    if ($composeText -notmatch "entrypoint:\s*\[\s*/usr/local/bin/tmdb-runtime\s*,\s*$role\s*\]") {
        throw "The $role service must start through the automatic tmdb-runtime storage preparer."
    }
}
if ($composeText -match 'entrypoint:\s*\[\s*/usr/local/bin/tmdb-(worker|images)') {
    throw 'Worker services must not bypass the automatic tmdb-runtime storage preparer.'
}
if ($composeText -notmatch 'cap_add:\s*\[\s*CHOWN\s*,\s*DAC_OVERRIDE\s*,\s*FOWNER\s*,\s*SETGID\s*,\s*SETUID\s*,\s*SETPCAP\s*\]') {
    throw 'Worker services must retain the minimal startup capabilities needed to prepare fixed storage paths and drop privileges.'
}

$temporaryOutput = [System.IO.Path]::GetTempFileName()
$previousTmdbEnvFile = [Environment]::GetEnvironmentVariable('TMDB_ENV_FILE', 'Process')
try {
    # The canonical template defaults to ../.env for normal deployment. Point
    # it at the caller-selected file during validation so this command tests
    # the exact settings it was given without creating a throwaway root .env.
    $env:TMDB_ENV_FILE = $envPath
    & docker compose --env-file $envPath --file $composePath config --quiet *> $temporaryOutput
    if ($LASTEXITCODE -ne 0) {
        throw 'Docker Compose rejected the production template or its interpolation.'
    }
}
finally {
    if ($null -eq $previousTmdbEnvFile) {
        Remove-Item Env:TMDB_ENV_FILE -ErrorAction SilentlyContinue
    }
    else {
        $env:TMDB_ENV_FILE = $previousTmdbEnvFile
    }
    Remove-Item -LiteralPath $temporaryOutput -Force -ErrorAction SilentlyContinue
}

Write-Output 'Production Compose interpolation and fixed four-container contracts passed.'
