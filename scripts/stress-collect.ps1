[CmdletBinding()]
param(
    [string]$ProjectName = 'tmdb_stress_test'
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $PSScriptRoot
. (Join-Path $repoRoot 'scripts/stress-secrets.ps1')
$composeFile = Join-Path $repoRoot 'deploy/compose.stress.yaml'
$runtimeRoot = Join-Path (Join-Path $repoRoot '.stress-runtime') $ProjectName
$envFile = Join-Path $runtimeRoot 'compose.env'
$metadataFile = Join-Path $runtimeRoot 'metadata.json'
$resultRoot = Join-Path $runtimeRoot 'results'
New-Item -ItemType Directory -Force -Path $resultRoot | Out-Null
$stamp = [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ')
$composeArgs = @('compose', '--env-file', $envFile, '--project-name', $ProjectName, '--file', $composeFile)

if (-not (Test-Path -LiteralPath $envFile -PathType Leaf)) {
    throw "Runtime environment is missing: $envFile"
}
if (-not (Test-Path -LiteralPath $metadataFile -PathType Leaf)) {
    throw "Stress runtime metadata is missing: $metadataFile"
}
$databaseIdentity = Read-StressDatabaseIdentity -Path $envFile
$metadata = Get-Content -Raw -LiteralPath $metadataFile | ConvertFrom-Json
if ([string]::IsNullOrWhiteSpace([string]$metadata.started_at_utc)) {
    throw "Stress runtime metadata does not contain started_at_utc: $metadataFile"
}
try {
    $startedAt = [DateTimeOffset]::Parse(
        [string]$metadata.started_at_utc,
        [Globalization.CultureInfo]::InvariantCulture,
        [Globalization.DateTimeStyles]::RoundtripKind
    )
}
catch {
    throw "Stress runtime metadata contains an invalid started_at_utc value: $metadataFile"
}
$startedAtUtc = $startedAt.UtcDateTime.ToString('O')

function Invoke-External {
    param([Parameter(Mandatory)][string[]]$Arguments)
    $output = @(& docker @Arguments 2>&1)
    $code = $LASTEXITCODE
    if ($code -ne 0) { throw "Docker command failed with exit code $code.`n$([string]::Join("`n", $output))" }
    return [string]::Join("`n", $output)
}

$ps = Invoke-External -Arguments ($composeArgs + @('ps', '--all'))
[System.IO.File]::WriteAllText((Join-Path $resultRoot "compose-$stamp.txt"), $ps, [System.Text.UTF8Encoding]::new($false))

$logs = Invoke-External -Arguments ($composeArgs + @('logs', '--no-color', '--timestamps'))
$logsPath = Join-Path $resultRoot "logs-$stamp.txt"
[System.IO.File]::WriteAllText($logsPath, $logs, [System.Text.UTF8Encoding]::new($false))

# The regression test covers crossed TV-detail and season writes at the
# database boundary. Scope the runtime check to PostgreSQL logs from this
# stress run so historical or application log text cannot create a false pass
# or false failure.
$postgresLogs = Invoke-External -Arguments ($composeArgs + @(
    'logs', '--no-color', '--timestamps', '--since', $startedAtUtc, 'postgres'
))
$postgresLogsPath = Join-Path $resultRoot "postgres-logs-$stamp.txt"
[System.IO.File]::WriteAllText($postgresLogsPath, $postgresLogs, [System.Text.UTF8Encoding]::new($false))
$writeContention = [regex]::Matches(
    $postgresLogs,
    '(?im)^.*\bERROR:\s+(?:deadlock detected|canceling statement due to lock timeout)\b.*$'
)
if ($writeContention.Count -gt 0) {
    $contentionPath = Join-Path $resultRoot "catalog-write-contention-$stamp.json"
    $contention = [ordered]@{
        checked_at_utc = [DateTime]::UtcNow.ToString('O')
        run_started_at_utc = $startedAtUtc
        postgres_log_path = $postgresLogsPath
        matches = @($writeContention | ForEach-Object { $_.Value.Trim() })
    }
    [System.IO.File]::WriteAllText(
        $contentionPath,
        (($contention | ConvertTo-Json -Depth 4) + "`n"),
        [System.Text.UTF8Encoding]::new($false)
    )
    throw "Catalog write contention was detected. See $contentionPath and $logsPath"
}

$stats = Invoke-External -Arguments @('stats', '--no-stream', '--format', '{{json .}}')
[System.IO.File]::WriteAllText((Join-Path $resultRoot "docker-stats-$stamp.jsonl"), $stats, [System.Text.UTF8Encoding]::new($false))

$statsSql = @'
SELECT json_build_object(
  'database_size_bytes', pg_database_size(current_database()),
  'database_size', pg_size_pretty(pg_database_size(current_database())),
  'titles', (SELECT count(*) FROM catalog.titles),
  'anime_titles', (SELECT count(*) FROM catalog.titles WHERE is_anime),
  'people', (SELECT count(*) FROM catalog.people),
  'search_documents', (SELECT count(*) FROM search.search_documents),
  'jobs_queued', (SELECT count(*) FROM ops.jobs WHERE status IN ('queued','retry_wait','running')),
  'jobs_succeeded', (SELECT count(*) FROM ops.jobs WHERE status = 'succeeded'),
  'jobs_failed', (SELECT count(*) FROM ops.jobs WHERE status = 'dead_letter'),
  'active_connections', (SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()),
  'cache_hit_ratio', (SELECT round(100.0 * sum(blks_hit) / nullif(sum(blks_hit + blks_read), 0), 3) FROM pg_stat_database WHERE datname = current_database())
);
'@
$passwordOutput = & docker @($composeArgs + @('exec', '-T', 'postgres', 'printenv', 'POSTGRES_PASSWORD')) 2>&1
if ($LASTEXITCODE -ne 0) {
    throw 'Unable to read the disposable database password from the container environment.'
}
$postgresPassword = ([string]::Join("`n", @($passwordOutput))).Trim()
if ([string]::IsNullOrWhiteSpace($postgresPassword)) {
    throw 'Disposable database password is empty.'
}
$dbStats = $statsSql | & docker @($composeArgs + @('exec', '-T', '-e', "PGPASSWORD=$postgresPassword", 'postgres', 'psql', '-X', '-At', '--username', $databaseIdentity.Username, '--dbname', $databaseIdentity.Database)) 2>&1
if ($LASTEXITCODE -ne 0) {
    throw "Database statistics query failed.`n$([string]::Join("`n", @($dbStats)))"
}
[System.IO.File]::WriteAllText((Join-Path $resultRoot "database-$stamp.json"), ([string]::Join("`n", @($dbStats)).Trim() + "`n"), [System.Text.UTF8Encoding]::new($false))

Write-Output "Collected stress artifacts under $resultRoot"
Write-Output "Compose status: $(Join-Path $resultRoot "compose-$stamp.txt")"
Write-Output "Database statistics: $(Join-Path $resultRoot "database-$stamp.json")"
