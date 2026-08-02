[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string]$ResultDirectory,

    [string]$ComposeFile = '',

    [string]$ComposeEnvFile = '',

    [string]$ComposeProjectName = '',

    [string]$AdminMetricsUrl = '',

    [ValidatePattern('^[A-Za-z_][A-Za-z0-9_]{0,127}$')]
    [string]$AdminKeyEnvironmentVariable = 'TMDB_K6_ADMIN_API_KEY',

    [string]$RunStartedAtUtc = ''
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Protect-Text {
    param(
        [AllowNull()][string]$Text,
        [string[]]$KnownSecrets = @()
    )

    if ($null -eq $Text) {
        return ''
    }

    $protected = $Text
    foreach ($secret in $KnownSecrets) {
        if (-not [string]::IsNullOrWhiteSpace($secret)) {
            $protected = $protected.Replace($secret, '<redacted>')
        }
    }
    $protected = [regex]::Replace(
        $protected,
        '(?im)\b(authorization|x-api-key)\s*[:=]\s*(?:bearer\s+)?[^\s,;]+',
        '$1=<redacted>'
    )
    $protected = [regex]::Replace(
        $protected,
        '(?im)\b(password|token|api[_-]?key|secret)\s*[:=]\s*[^\s,;]+',
        '$1=<redacted>'
    )
    $protected = [regex]::Replace(
        $protected,
        '([A-Za-z][A-Za-z0-9+.-]*://[^\s:/]+:)[^@\s/]+@',
        '$1<redacted>@'
    )
    $protected = [regex]::Replace(
        $protected,
        '\beyJ[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+){2}\b',
        '<redacted-token>'
    )
    return $protected
}

function Write-Utf8NoBom {
    param(
        [Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][string]$Content
    )

    [IO.File]::WriteAllText($Path, $Content, [Text.UTF8Encoding]::new($false))
}

function Invoke-DockerText {
    param(
        [Parameter(Mandatory)][string[]]$Arguments,
        [string[]]$KnownSecrets = @()
    )

    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $rawOutput = @(& docker @Arguments 2>&1)
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    return [pscustomobject]@{
        exit_code = $exitCode
        output = Protect-Text -Text ([string]::Join("`n", @($rawOutput))) -KnownSecrets $KnownSecrets
    }
}

function Get-ProjectContainerIds {
    param(
        [AllowEmptyString()][string]$ProjectName,
        [string]$Service = ''
    )

    if ([string]::IsNullOrWhiteSpace($ProjectName)) {
        return @()
    }

    $arguments = [Collections.Generic.List[string]]::new()
    foreach ($entry in @('ps', '-q', '--filter', "label=com.docker.compose.project=$ProjectName")) {
        [void]$arguments.Add($entry)
    }
    if (-not [string]::IsNullOrWhiteSpace($Service)) {
        [void]$arguments.Add('--filter')
        [void]$arguments.Add("label=com.docker.compose.service=$Service")
    }

    $result = Invoke-DockerText -Arguments $arguments.ToArray()
    if ($result.exit_code -ne 0) {
        return @()
    }
    return @(
        $result.output -split "`r?`n" |
            ForEach-Object { $_.Trim() } |
            Where-Object { $_ -match '^[0-9a-f]{12,64}$' }
    )
}

function Invoke-PostgresText {
    param(
        [Parameter(Mandatory)][string]$ContainerId,
        [Parameter(Mandatory)][string]$Sql,
        [string[]]$KnownSecrets = @()
    )

    $command = 'exec env PGPASSWORD="$POSTGRES_PASSWORD" psql -X -v ON_ERROR_STOP=1 -At --username "$POSTGRES_USER" --dbname "$POSTGRES_DB"'
    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $rawOutput = @($Sql | & docker exec -i $ContainerId sh -ec $command 2>&1)
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    return [pscustomobject]@{
        exit_code = $exitCode
        output = Protect-Text -Text ([string]::Join("`n", @($rawOutput))) -KnownSecrets $KnownSecrets
    }
}

function ConvertTo-SafeHttpUrl {
    param([Parameter(Mandatory)][string]$Value)

    $uri = $null
    if (-not [Uri]::TryCreate($Value.Trim(), [UriKind]::Absolute, [ref]$uri) -or
        $uri.Scheme -notin @('http', 'https') -or
        [string]::IsNullOrWhiteSpace($uri.Host) -or
        -not [string]::IsNullOrWhiteSpace($uri.UserInfo) -or
        -not [string]::IsNullOrWhiteSpace($uri.Query) -or
        -not [string]::IsNullOrWhiteSpace($uri.Fragment)) {
        throw 'AdminMetricsUrl must be an http(s) URL without credentials, a query string, or a fragment.'
    }
    return $uri.AbsoluteUri
}

if (-not (Test-Path -LiteralPath $ResultDirectory -PathType Container)) {
    throw "Result directory is missing: $ResultDirectory"
}
foreach ($pathSetting in @($ComposeFile, $ComposeEnvFile)) {
    if (-not [string]::IsNullOrWhiteSpace($pathSetting) -and -not (Test-Path -LiteralPath $pathSetting -PathType Leaf)) {
        throw "Configured Compose file does not exist: $pathSetting"
    }
}
if (-not [string]::IsNullOrWhiteSpace($ComposeProjectName) -and $ComposeProjectName -notmatch '^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$') {
    throw 'ComposeProjectName must be a Docker Compose project name.'
}

$adminKey = [Environment]::GetEnvironmentVariable($AdminKeyEnvironmentVariable, 'Process')
$knownSecrets = @($adminKey | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
$timestamp = [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ')
$diagnosticPath = Join-Path $ResultDirectory "k6-failure-diagnostics-$timestamp.json"

$diagnostic = [ordered]@{
    schema_version = 1
    captured_at_utc = [DateTime]::UtcNow.ToString('O')
    run_started_at_utc = $RunStartedAtUtc
    compose_project = if ([string]::IsNullOrWhiteSpace($ComposeProjectName)) { $null } else { $ComposeProjectName }
    compose = [ordered]@{ status = 'not_requested' }
    docker = [ordered]@{
        status = 'not_requested'
        container_ids = @()
        stats = $null
        logs = @{}
    }
    postgres = [ordered]@{
        status = 'not_requested'
        wait_snapshot = $null
        pg_stat_statements = $null
        plans = [ordered]@{}
    }
    application_pool_metrics = [ordered]@{ status = 'not_requested'; lines = @() }
}

if (-not [string]::IsNullOrWhiteSpace($ComposeFile)) {
    $composeArguments = [Collections.Generic.List[string]]::new()
    [void]$composeArguments.Add('compose')
    if (-not [string]::IsNullOrWhiteSpace($ComposeEnvFile)) {
        [void]$composeArguments.Add('--env-file')
        [void]$composeArguments.Add($ComposeEnvFile)
    }
    if (-not [string]::IsNullOrWhiteSpace($ComposeProjectName)) {
        [void]$composeArguments.Add('--project-name')
        [void]$composeArguments.Add($ComposeProjectName)
    }
    [void]$composeArguments.Add('--file')
    [void]$composeArguments.Add($ComposeFile)
    [void]$composeArguments.Add('ps')
    [void]$composeArguments.Add('--all')
    $composeStatus = Invoke-DockerText -Arguments $composeArguments.ToArray() -KnownSecrets $knownSecrets
    $diagnostic.compose = [ordered]@{
        status = if ($composeStatus.exit_code -eq 0) { 'captured' } else { 'error' }
        exit_code = $composeStatus.exit_code
        output = $composeStatus.output
    }
}

$containerIds = @(Get-ProjectContainerIds -ProjectName $ComposeProjectName)
$diagnostic.docker.container_ids = $containerIds
if ($containerIds.Count -gt 0) {
    $diagnostic.docker.status = 'captured'
    $statsArguments = [Collections.Generic.List[string]]::new()
    foreach ($entry in @('stats', '--no-stream', '--format', '{{json .}}')) {
        [void]$statsArguments.Add($entry)
    }
    foreach ($containerId in $containerIds) {
        [void]$statsArguments.Add($containerId)
    }
    $stats = Invoke-DockerText -Arguments $statsArguments.ToArray() -KnownSecrets $knownSecrets
    $diagnostic.docker.stats = [ordered]@{
        exit_code = $stats.exit_code
        output = $stats.output
    }

    foreach ($containerId in $containerIds) {
        $logArguments = [Collections.Generic.List[string]]::new()
        foreach ($entry in @('logs', '--timestamps')) {
            [void]$logArguments.Add($entry)
        }
        if (-not [string]::IsNullOrWhiteSpace($RunStartedAtUtc)) {
            [void]$logArguments.Add('--since')
            [void]$logArguments.Add($RunStartedAtUtc)
        }
        [void]$logArguments.Add($containerId)
        $logs = Invoke-DockerText -Arguments $logArguments.ToArray() -KnownSecrets $knownSecrets
        $lines = @($logs.output -split "`r?`n")
        if ($lines.Count -gt 500) {
            $lines = @($lines | Select-Object -Last 500)
        }
        $diagnostic.docker.logs[$containerId] = [ordered]@{
            exit_code = $logs.exit_code
            output = [string]::Join("`n", $lines)
        }
    }
}

$postgresContainer = @(
    Get-ProjectContainerIds -ProjectName $ComposeProjectName -Service 'postgres' |
        Select-Object -First 1
)
if ($postgresContainer.Count -gt 0) {
    $postgresId = [string]$postgresContainer[0]
    $diagnostic.postgres.status = 'capturing'

    $waitSql = @'
WITH waiting AS (
    SELECT pid, usename, application_name, state, wait_event_type, wait_event,
           clock_timestamp() - query_start AS query_age
    FROM pg_stat_activity
    WHERE datname = current_database()
      AND pid <> pg_backend_pid()
      AND wait_event_type IS NOT NULL
), blocked AS (
    SELECT blocked_activity.pid,
           blocked_activity.usename,
           blocked_activity.application_name,
           blocked_activity.wait_event_type,
           blocked_activity.wait_event,
           blocking_activity.pid AS blocking_pid,
           blocking_activity.usename AS blocking_usename
    FROM pg_locks blocked_locks
    JOIN pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
    JOIN pg_locks blocking_locks
      ON blocking_locks.locktype = blocked_locks.locktype
     AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
     AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
     AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
     AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
     AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
     AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
     AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
     AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
     AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
     AND blocking_locks.pid <> blocked_locks.pid
    JOIN pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
    WHERE NOT blocked_locks.granted AND blocking_locks.granted
)
SELECT json_build_object(
    'waiting_backends', coalesce((SELECT json_agg(row_to_json(w)) FROM waiting AS w), '[]'::json),
    'blocked_backends', coalesce((SELECT json_agg(row_to_json(b)) FROM blocked AS b), '[]'::json)
);
'@
    $waits = Invoke-PostgresText -ContainerId $postgresId -Sql $waitSql -KnownSecrets $knownSecrets
    $diagnostic.postgres.wait_snapshot = [ordered]@{
        exit_code = $waits.exit_code
        output = $waits.output
    }

    $statementsSql = @'
SELECT coalesce(json_agg(row_to_json(statement)), '[]'::json)
FROM (
    SELECT calls,
           total_exec_time,
           mean_exec_time,
           rows,
           left(query, 500) AS query
    FROM pg_stat_statements
    WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
    ORDER BY total_exec_time DESC
    LIMIT 25
) AS statement;
'@
    $statements = Invoke-PostgresText -ContainerId $postgresId -Sql $statementsSql -KnownSecrets $knownSecrets
    $diagnostic.postgres.pg_stat_statements = [ordered]@{
        exit_code = $statements.exit_code
        output = $statements.output
    }

    $planQueries = [ordered]@{
        list = @'
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT id
FROM catalog.titles
WHERE media_type = 'movie' AND active AND NOT is_anime
ORDER BY coalesce(popularity, 0::double precision) DESC, id DESC
LIMIT 20;
'@
        search = @'
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT title_id
FROM search.search_documents
WHERE search_vector @@ websearch_to_tsquery('simple', 'cafe')
LIMIT 20;
'@
        filter = @'
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT title.id
FROM catalog.titles AS title
JOIN catalog.title_genres AS title_genre ON title_genre.title_id = title.id
WHERE title.media_type = 'movie'
  AND title.active
  AND NOT title.is_anime
  AND title_genre.genre_id = 900000002
ORDER BY coalesce(title.popularity, 0::double precision) DESC, title.id DESC
LIMIT 20;
'@
    }
    foreach ($planName in $planQueries.Keys) {
        $plan = Invoke-PostgresText -ContainerId $postgresId -Sql $planQueries[$planName] -KnownSecrets $knownSecrets
        $diagnostic.postgres.plans[$planName] = [ordered]@{
            exit_code = $plan.exit_code
            output = $plan.output
        }
    }
    $diagnostic.postgres.status = 'captured'
}

if (-not [string]::IsNullOrWhiteSpace($AdminMetricsUrl)) {
    $safeMetricsUrl = ConvertTo-SafeHttpUrl -Value $AdminMetricsUrl
    if ([string]::IsNullOrWhiteSpace($adminKey)) {
        $diagnostic.application_pool_metrics = [ordered]@{
            status = 'skipped_missing_admin_key'
            lines = @()
        }
    }
    else {
        try {
            $response = Invoke-WebRequest -UseBasicParsing -Uri $safeMetricsUrl -Method Get -TimeoutSec 10 `
                -Headers @{ 'X-API-Key' = $adminKey }
            $lines = @(
                [string]$response.Content -split "`r?`n" |
                    Where-Object { $_ -match '(?i)(pool|wait|queue|worker|upstream|backup)' } |
                    Select-Object -First 500 |
                    ForEach-Object { Protect-Text -Text $_ -KnownSecrets $knownSecrets }
            )
            $diagnostic.application_pool_metrics = [ordered]@{
                status = 'captured'
                http_status = [int]$response.StatusCode
                lines = $lines
            }
        }
        catch {
            $diagnostic.application_pool_metrics = [ordered]@{
                status = 'error'
                detail = Protect-Text -Text $_.Exception.Message -KnownSecrets $knownSecrets
                lines = @()
            }
        }
    }
}

$serialized = $diagnostic | ConvertTo-Json -Depth 20
Write-Utf8NoBom -Path $diagnosticPath -Content ($serialized + "`n")
Write-Output "Redacted k6 failure diagnostics: $diagnosticPath"
