[CmdletBinding()]
param(
    [string]$ProjectName = 'tmdb_stress_test',
    [ValidateRange(1000, 2000000)][int]$Count = 100000
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $PSScriptRoot
. (Join-Path $repoRoot 'scripts/stress-secrets.ps1')
$composeFile = Join-Path $repoRoot 'deploy/compose.stress.yaml'
$runtimeRoot = Join-Path (Join-Path $repoRoot '.stress-runtime') $ProjectName
$envFile = Join-Path $runtimeRoot 'compose.env'
$seedFile = Join-Path $repoRoot 'scripts/stress-seed.sql'
$baseId = 900000000

if (-not (Test-Path -LiteralPath $envFile -PathType Leaf)) {
    throw "Runtime environment is missing: $envFile. Run stress-bootstrap.ps1 first."
}
if (-not (Test-Path -LiteralPath $seedFile -PathType Leaf)) {
    throw "Seed SQL is missing: $seedFile"
}
$databaseIdentity = Read-StressDatabaseIdentity -Path $envFile

$composeArgs = @('compose', '--env-file', $envFile, '--project-name', $ProjectName, '--file', $composeFile)
$sql = [IO.File]::ReadAllText($seedFile, [Text.UTF8Encoding]::new($false))
$passwordOutput = & docker @($composeArgs + @('exec', '-T', 'postgres', 'printenv', 'POSTGRES_PASSWORD')) 2>&1
if ($LASTEXITCODE -ne 0) {
    throw "Unable to read the disposable database password from the container environment."
}
$postgresPassword = ([string]::Join("`n", @($passwordOutput))).Trim()
if ([string]::IsNullOrWhiteSpace($postgresPassword)) {
    throw 'Disposable database password is empty.'
}
$psqlArgs = $composeArgs + @('exec', '-T', '-e', "PGPASSWORD=$postgresPassword", 'postgres', 'psql', '-X', '-v', 'ON_ERROR_STOP=1', '--username', $databaseIdentity.Username, '--dbname', $databaseIdentity.Database, '--set', "seed_count=$Count", '--set', "seed_base=$baseId")
$containerId = ([string]::Join("`n", @(& docker @($composeArgs + @('ps', '-q', 'postgres'))))).Trim()
if ([string]::IsNullOrWhiteSpace($containerId)) {
    throw 'Unable to resolve the disposable postgres container.'
}
$containerSeedPath = '/tmp/tmdb-stress-seed.sql'
& docker cp $seedFile "${containerId}:$containerSeedPath" 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw 'Unable to copy UTF-8 seed SQL into the disposable postgres container.'
}
try {
    $output = & docker @($psqlArgs + @('--file', $containerSeedPath)) 2>&1
}
finally {
    & docker @($composeArgs + @('exec', '-T', 'postgres', 'rm', '-f', $containerSeedPath)) 2>&1 | Out-Null
}
$exitCode = $LASTEXITCODE
if ($exitCode -ne 0) {
    throw "Synthetic seed failed with exit code $exitCode.`n$([string]::Join("`n", @($output)))"
}

$verificationSql = 'SELECT json_build_object(''titles'', (SELECT count(*) FROM catalog.titles WHERE tmdb_id >= {0} + 1 AND tmdb_id < {0} + {1} + 1), ''anime'', (SELECT count(*) FROM catalog.titles WHERE is_anime AND tmdb_id >= {0} + 1 AND tmdb_id < {0} + {1} + 1), ''search_documents'', (SELECT count(*) FROM search.search_documents WHERE title_id IN (SELECT id FROM catalog.titles WHERE tmdb_id >= {0} + 1 AND tmdb_id < {0} + {1} + 1)))' -f $baseId, $Count
$countOutput = & docker @($composeArgs + @('exec', '-T', '-e', "PGPASSWORD=$postgresPassword", 'postgres', 'psql', '-X', '-At', '--username', $databaseIdentity.Username, '--dbname', $databaseIdentity.Database, '-c', $verificationSql)) 2>&1
if ($LASTEXITCODE -ne 0) {
    throw "Seed verification failed.`n$([string]::Join("`n", @($countOutput)))"
}
Write-Output ([string]::Join("`n", @($countOutput)).Trim())
