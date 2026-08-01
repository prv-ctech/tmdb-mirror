[CmdletBinding()]
param(
    [string]$ProjectName = 'tmdb_stress_test'
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $repoRoot 'deploy/compose.stress.yaml'
$runtimeRoot = Join-Path (Join-Path $repoRoot '.stress-runtime') $ProjectName
$envFile = Join-Path $runtimeRoot 'compose.env'
$resultRoot = Join-Path $runtimeRoot 'results'
New-Item -ItemType Directory -Force -Path $resultRoot | Out-Null
$stamp = [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ')
$composeArgs = @('compose', '--env-file', $envFile, '--project-name', $ProjectName, '--file', $composeFile)

if (-not (Test-Path -LiteralPath $envFile -PathType Leaf)) {
    throw "Runtime environment is missing: $envFile"
}

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
[System.IO.File]::WriteAllText((Join-Path $resultRoot "logs-$stamp.txt"), $logs, [System.Text.UTF8Encoding]::new($false))

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
$passwordOutput = & docker @($composeArgs + @('exec', '-T', 'postgres', 'cat', '/run/secrets/postgres_owner_password')) 2>&1
if ($LASTEXITCODE -ne 0) {
    throw 'Unable to read the disposable database password from the container.'
}
$postgresPassword = ([string]::Join("`n", @($passwordOutput))).Trim()
if ([string]::IsNullOrWhiteSpace($postgresPassword)) {
    throw 'Disposable database password is empty.'
}
$dbStats = $statsSql | & docker @($composeArgs + @('exec', '-T', '-e', "PGPASSWORD=$postgresPassword", 'postgres', 'psql', '-X', '-At', '--username', 'tmdb_owner', '--dbname', 'tmdb')) 2>&1
if ($LASTEXITCODE -ne 0) {
    throw "Database statistics query failed.`n$([string]::Join("`n", @($dbStats)))"
}
[System.IO.File]::WriteAllText((Join-Path $resultRoot "database-$stamp.json"), ([string]::Join("`n", @($dbStats)).Trim() + "`n"), [System.Text.UTF8Encoding]::new($false))

Write-Output "Collected stress artifacts under $resultRoot"
Write-Output "Compose status: $(Join-Path $resultRoot "compose-$stamp.txt")"
Write-Output "Database statistics: $(Join-Path $resultRoot "database-$stamp.json")"
