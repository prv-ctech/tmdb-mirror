[CmdletBinding()]
param(
    [string]$ProjectName = 'tmdb_stress_test',
    [int]$ApiPort = 18080,
    [int]$ImagePort = 18090,
    [int]$TimeoutSeconds = 180,
    [string]$SecretsFile
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
Add-Type -AssemblyName System.Net.Http

$repoRoot = Split-Path -Parent $PSScriptRoot
. (Join-Path $repoRoot 'scripts/stress-secrets.ps1')
$composeFile = Join-Path $repoRoot 'deploy/compose.stress.yaml'
$runtimeRoot = Join-Path (Join-Path $repoRoot '.stress-runtime') $ProjectName
$envFile = Join-Path $runtimeRoot 'compose.env'
$resultRoot = Join-Path $runtimeRoot 'results'
$stamp = [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ')
$resultFile = Join-Path $resultRoot "artwork-$stamp.json"
$apiBaseUrl = "http://127.0.0.1:$ApiPort"
$imageBaseUrl = "http://127.0.0.1:$ImagePort"

if (-not (Test-Path -LiteralPath $envFile -PathType Leaf)) {
    throw "Runtime environment is missing: $envFile. Run stress-bootstrap.ps1 first."
}
$databaseIdentity = Read-StressDatabaseIdentity -Path $envFile
if ([string]::IsNullOrWhiteSpace($SecretsFile)) {
    $SecretsFile = Join-Path $repoRoot 'secrets.txt'
}
elseif (-not (Test-Path -LiteralPath $SecretsFile -PathType Leaf)) {
    throw "Local stress secrets file is missing: $SecretsFile"
}
New-Item -ItemType Directory -Force -Path $resultRoot | Out-Null

$secrets = Read-StressSecrets -Path $SecretsFile
$readToken = Resolve-StressSecret `
    -Secrets $secrets -Name 'TMDB_STRESS_READ_TOKEN' -ExplicitValue $env:TMDB_STRESS_READ_TOKEN
if ([string]::IsNullOrWhiteSpace($readToken)) {
    throw 'The ignored secrets file must provide TMDB_STRESS_READ_TOKEN.'
}

$composeArgs = @('compose', '--env-file', $envFile, '--project-name', $ProjectName, '--file', $composeFile)

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
        throw 'Docker failed during the real-artwork stress check.'
    }
    return [string]::Join("`n", $output).Trim()
}

function Invoke-PostgresJson {
    param(
        [Parameter(Mandatory)][string]$Password,
        [Parameter(Mandatory)][string]$Sql
    )

    $output = Invoke-DockerChecked -Arguments ($composeArgs + @(
        'exec', '-T', '-e', "PGPASSWORD=$Password", 'postgres', 'psql', '-X', '-At',
        '--username', $databaseIdentity.Username, '--dbname', $databaseIdentity.Database, '-c', $Sql
    ))
    if ([string]::IsNullOrWhiteSpace($output)) {
        throw 'PostgreSQL returned no JSON during the real-artwork stress check.'
    }
    try {
        return ($output | ConvertFrom-Json)
    }
    catch {
        throw 'PostgreSQL returned invalid JSON during the real-artwork stress check.'
    }
}

function Submit-Refresh {
    param(
        [Parameter(Mandatory)][ValidateSet('movie', 'tv')][string]$MediaType,
        [Parameter(Mandatory)][int]$TmdbId
    )

    $output = Invoke-DockerChecked -Arguments ($composeArgs + @(
        'run', '--rm', '--no-deps', '--entrypoint', '/usr/local/bin/tmdb-admin', 'worker',
        'submit-refresh', '--media-type', $MediaType, '--tmdb-id', $TmdbId.ToString()
    ))
    $jsonLine = @($output -split "`r?`n" | Where-Object { $_.Trim().StartsWith('{') } | Select-Object -Last 1)
    if ($jsonLine.Count -ne 1) {
        throw 'The refresh submission returned no machine-readable result.'
    }
    try {
        $job = $jsonLine[0] | ConvertFrom-Json
    }
    catch {
        throw 'The refresh submission returned invalid JSON.'
    }
    if ([string]$job.job_id -notmatch '^[0-9a-fA-F-]{36}$') {
        throw 'The refresh submission returned an invalid job identity.'
    }
    return $job
}

function Get-RefreshStates {
    param(
        [Parameter(Mandatory)][string]$Password,
        [Parameter(Mandatory)][string[]]$JobIds
    )

    $quotedIds = foreach ($jobId in $JobIds) {
        if ($jobId -notmatch '^[0-9a-fA-F-]{36}$') {
            throw 'Invalid refresh job identity.'
        }
        "'$jobId'::uuid"
    }
    $sql = @"
SELECT COALESCE(
    json_agg(json_build_object('id', id::text, 'status', status) ORDER BY id),
    '[]'::json
)::text
FROM ops.jobs
WHERE id IN ($($quotedIds -join ','));
"@
    return @(Invoke-PostgresJson -Password $Password -Sql $sql)
}

function Get-TmdbDetail {
    param(
        [Parameter(Mandatory)][System.Net.Http.HttpClient]$Client,
        [Parameter(Mandatory)][ValidateSet('movie', 'tv')][string]$MediaType,
        [Parameter(Mandatory)][int]$TmdbId,
        [Parameter(Mandatory)][string]$Token
    )

    $uri = [System.UriBuilder]::new('https', 'api.themoviedb.org', 443, "/3/$MediaType/$TmdbId")
    $uri.Query = 'append_to_response=keywords'
    $request = [System.Net.Http.HttpRequestMessage]::new([System.Net.Http.HttpMethod]::Get, $uri.Uri)
    $response = $null
    try {
        $request.Headers.Authorization = [System.Net.Http.Headers.AuthenticationHeaderValue]::new('Bearer', $Token)
        $response = $Client.SendAsync($request).GetAwaiter().GetResult()
        if (-not $response.IsSuccessStatusCode) {
            throw "TMDB detail preflight returned HTTP $([int]$response.StatusCode)."
        }
        $payload = $response.Content.ReadAsStringAsync().GetAwaiter().GetResult() | ConvertFrom-Json
        $keywords = if ($MediaType -eq 'movie') { @($payload.keywords.keywords) } else { @($payload.keywords.results) }
        return [ordered]@{
            title = if ($MediaType -eq 'movie') { [string]$payload.title } else { [string]$payload.name }
            is_anime = @($keywords | Where-Object { $_.id -eq 210024 }).Count -gt 0
            poster_path = [string]$payload.poster_path
            backdrop_path = [string]$payload.backdrop_path
        }
    }
    finally {
        if ($null -ne $response) { $response.Dispose() }
        $request.Dispose()
    }
}

function Get-TargetAssets {
    param([Parameter(Mandatory)][string]$Password)

    $sql = @'
WITH requested(media_type, tmdb_id) AS (
    VALUES
        ('movie'::text, 550::bigint),
        ('tv'::text, 1399::bigint),
        ('tv'::text, 37854::bigint),
        ('movie'::text, 900667::bigint)
), rows AS (
    SELECT
        requested.media_type,
        requested.tmdb_id,
        title.is_anime,
        COALESCE((
            SELECT json_agg(json_build_object(
                'image_kind', asset.image_kind,
                'source_key', asset.source_key,
                'storage_path', asset.storage_path,
                'sha256', asset.sha256,
                'status', asset.status
            ) ORDER BY asset.image_kind)
            FROM assets.image_assets AS asset
            WHERE asset.title_id = title.id
              AND asset.image_kind IN ('poster', 'backdrop')
        ), '[]'::json) AS assets
    FROM requested
    LEFT JOIN catalog.titles AS title
      ON title.media_type = requested.media_type
     AND title.tmdb_id = requested.tmdb_id
     AND title.active
)
SELECT COALESCE(json_agg(row_to_json(rows) ORDER BY media_type, tmdb_id), '[]'::json)::text
FROM rows;
'@
    return @(Invoke-PostgresJson -Password $Password -Sql $sql)
}

function Get-HttpStatus {
    param(
        [Parameter(Mandatory)][System.Net.Http.HttpClient]$Client,
        [Parameter(Mandatory)][string]$Url
    )

    $response = $null
    try {
        $response = $Client.GetAsync($Url).GetAwaiter().GetResult()
        $null = $response.Content.ReadAsByteArrayAsync().GetAwaiter().GetResult()
        return [int]$response.StatusCode
    }
    finally {
        if ($null -ne $response) { $response.Dispose() }
    }
}

function Get-JsonResponse {
    param(
        [Parameter(Mandatory)][System.Net.Http.HttpClient]$Client,
        [Parameter(Mandatory)][string]$Url
    )

    $response = $null
    try {
        $response = $Client.GetAsync($Url).GetAwaiter().GetResult()
        if (-not $response.IsSuccessStatusCode) {
            throw "API image metadata returned HTTP $([int]$response.StatusCode)."
        }
        return ($response.Content.ReadAsStringAsync().GetAwaiter().GetResult() | ConvertFrom-Json)
    }
    finally {
        if ($null -ne $response) { $response.Dispose() }
    }
}

$targets = @(
    [ordered]@{ media_type = 'movie'; tmdb_id = 550; expected_anime = $false; route = 'movies'; scope = 'movies' },
    [ordered]@{ media_type = 'tv'; tmdb_id = 1399; expected_anime = $false; route = 'tv'; scope = 'tv' },
    [ordered]@{ media_type = 'tv'; tmdb_id = 37854; expected_anime = $true; route = 'anime/tv'; scope = 'anime/tv' },
    [ordered]@{ media_type = 'movie'; tmdb_id = 900667; expected_anime = $true; route = 'anime/movie'; scope = 'anime/movie' }
)

$tmdbClient = [System.Net.Http.HttpClient]::new()
$tmdbClient.Timeout = [TimeSpan]::FromSeconds(30)
$localClient = [System.Net.Http.HttpClient]::new()
$localClient.Timeout = [TimeSpan]::FromSeconds(15)
$postgresPassword = $null
try {
    foreach ($target in $targets) {
        $detail = Get-TmdbDetail -Client $tmdbClient -MediaType $target.media_type -TmdbId $target.tmdb_id -Token $readToken
        if ($detail.is_anime -ne $target.expected_anime -or
            [string]::IsNullOrWhiteSpace($detail.poster_path) -or
            [string]::IsNullOrWhiteSpace($detail.backdrop_path)) {
            throw "TMDB artwork preflight did not satisfy the expected target contract for $($target.media_type)/$($target.tmdb_id)."
        }
        $target.title = $detail.title
        $target.poster_path = $detail.poster_path
        $target.backdrop_path = $detail.backdrop_path
    }

    $postgresPassword = Invoke-DockerChecked -Arguments ($composeArgs + @('exec', '-T', 'postgres', 'printenv', 'POSTGRES_PASSWORD'))
    if ([string]::IsNullOrWhiteSpace($postgresPassword)) {
        throw 'The disposable PostgreSQL password is unavailable.'
    }
    $submitted = foreach ($target in $targets) {
        Submit-Refresh -MediaType $target.media_type -TmdbId $target.tmdb_id
    }
    $jobIds = @($submitted | ForEach-Object { [string]$_.job_id })
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        $states = Get-RefreshStates -Password $postgresPassword -JobIds $jobIds
        if ($states.Count -eq $jobIds.Count -and @($states | Where-Object { $_.status -eq 'succeeded' }).Count -eq $jobIds.Count) {
            break
        }
        if (@($states | Where-Object { $_.status -in @('dead_letter', 'cancelled') }).Count -gt 0) {
            throw 'A real-TMDB refresh job did not complete successfully.'
        }
        Start-Sleep -Seconds 1
    } while ([DateTime]::UtcNow -lt $deadline)
    if ($states.Count -ne $jobIds.Count -or @($states | Where-Object { $_.status -eq 'succeeded' }).Count -ne $jobIds.Count) {
        throw 'Timed out waiting for real-TMDB refresh jobs.'
    }

    do {
        $assetRows = Get-TargetAssets -Password $postgresPassword
        $ready = $true
        foreach ($target in $targets) {
            $record = @($assetRows | Where-Object { $_.media_type -eq $target.media_type -and [int]$_.tmdb_id -eq $target.tmdb_id } | Select-Object -First 1)
            if ($record.Count -ne 1 -or $record[0].is_anime -ne $target.expected_anime) {
                $ready = $false
                break
            }
            foreach ($kind in @('poster', 'backdrop')) {
                $asset = @($record[0].assets | Where-Object { $_.image_kind -eq $kind } | Select-Object -First 1)
                if ($asset.Count -ne 1 -or $asset[0].status -ne 'ready') {
                    $ready = $false
                    break
                }
            }
            if (-not $ready) { break }
        }
        if ($ready) { break }
        Start-Sleep -Seconds 1
    } while ([DateTime]::UtcNow -lt $deadline)
    if (-not $ready) {
        throw 'Timed out waiting for real artwork to become ready.'
    }

    $checks = [System.Collections.Generic.List[object]]::new()
    $primaryHashes = [System.Collections.Generic.List[string]]::new()
    foreach ($target in $targets) {
        $record = @($assetRows | Where-Object { $_.media_type -eq $target.media_type -and [int]$_.tmdb_id -eq $target.tmdb_id } | Select-Object -First 1)[0]
        $metadataUrl = "$apiBaseUrl/$($target.route)/$($target.tmdb_id)/images"
        $metadataStatus = Get-HttpStatus -Client $localClient -Url $metadataUrl
        $metadata = Get-JsonResponse -Client $localClient -Url $metadataUrl
        $wrongRouteStatus = $null
        if ($target.expected_anime) {
            $wrongRoute = if ($target.media_type -eq 'movie') { 'movies' } else { 'tv' }
            $wrongRouteStatus = Get-HttpStatus -Client $localClient -Url "$apiBaseUrl/$wrongRoute/$($target.tmdb_id)/images"
        }

        foreach ($expectation in @(
            [ordered]@{ kind = 'poster'; source_key = $target.poster_path; storage_path = "$($target.scope)/$($target.tmdb_id)/cover.jpg" },
            [ordered]@{ kind = 'backdrop'; source_key = $target.backdrop_path; storage_path = "$($target.scope)/$($target.tmdb_id)/banner.jpg" }
        )) {
            $asset = @($record.assets | Where-Object { $_.image_kind -eq $expectation.kind } | Select-Object -First 1)[0]
            $localUrl = "$imageBaseUrl/media/$($asset.storage_path)"
            $localStatus = Get-HttpStatus -Client $localClient -Url $localUrl
            $metadataUrls = @($metadata.data | ForEach-Object { [string]$_.url })
            $passed = [bool](
                $record.is_anime -eq $target.expected_anime -and
                $asset.status -eq 'ready' -and
                $asset.source_key -eq $expectation.source_key -and
                $asset.storage_path -eq $expectation.storage_path -and
                $asset.sha256 -match '^[0-9a-f]{64}$' -and
                $metadataStatus -eq 200 -and
                $metadataUrls -contains $localUrl -and
                $localStatus -eq 200 -and
                ($null -eq $wrongRouteStatus -or $wrongRouteStatus -eq 404)
            )
            if ($expectation.kind -eq 'poster') {
                [void]$primaryHashes.Add([string]$asset.sha256)
            }
            $checks.Add([ordered]@{
                media_type = $target.media_type
                tmdb_id = $target.tmdb_id
                title = $target.title
                anime = $target.expected_anime
                kind = $expectation.kind
                source_key_matches_tmdb = ($asset.source_key -eq $expectation.source_key)
                storage_path = $asset.storage_path
                sha256 = $asset.sha256
                metadata_http = $metadataStatus
                local_file_http = $localStatus
                wrong_partition_http = $wrongRouteStatus
                passed = $passed
            })
        }
    }
    $uniquePrimaryHashes = @($primaryHashes | Sort-Object -Unique)
    $result = [ordered]@{
        checked_at_utc = [DateTime]::UtcNow.ToString('O')
        checks = @($checks)
        primary_poster_hashes_unique = ($uniquePrimaryHashes.Count -eq $targets.Count)
        passed = [bool](
            @($checks | Where-Object { -not $_.passed }).Count -eq 0 -and
            $uniquePrimaryHashes.Count -eq $targets.Count
        )
    }
    [IO.File]::WriteAllText($resultFile, (($result | ConvertTo-Json -Depth 8) + "`n"), [Text.UTF8Encoding]::new($false))
    Write-Output ($result | ConvertTo-Json -Depth 8)
    Write-Output "Real-artwork stress artifact: $resultFile"
    if (-not $result.passed) {
        exit 2
    }
}
finally {
    $tmdbClient.Dispose()
    $localClient.Dispose()
    $readToken = $null
    $secrets = $null
    $postgresPassword = $null
    Remove-Item Env:TMDB_STRESS_READ_TOKEN -ErrorAction SilentlyContinue
}
