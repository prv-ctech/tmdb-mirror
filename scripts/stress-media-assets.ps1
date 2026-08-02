[CmdletBinding()]
param(
    [string]$ProjectName = 'tmdb_stress_test',
    [int]$ImagePort = 18090,
    [ValidateRange(1, 32)][int]$ExpectedWorkers = 4
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
Add-Type -AssemblyName System.Net.Http

$repoRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $repoRoot 'deploy/compose.stress.yaml'
$runtimeRoot = Join-Path (Join-Path $repoRoot '.stress-runtime') $ProjectName
$envFile = Join-Path $runtimeRoot 'compose.env'
$resultRoot = Join-Path $runtimeRoot 'results'
$stamp = [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ')
$resultFile = Join-Path $resultRoot "media-assets-$stamp.json"

if (-not (Test-Path -LiteralPath $envFile -PathType Leaf)) {
    throw "Runtime environment is missing: $envFile. Run stress-bootstrap.ps1 first."
}
New-Item -ItemType Directory -Force -Path $resultRoot | Out-Null

$composeArgs = @('compose', '--env-file', $envFile, '--project-name', $ProjectName, '--file', $composeFile)
$script:verificationStage = 'initialization'

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
        throw "Docker failed during the media-asset verification at stage: $script:verificationStage."
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
        '--username', 'tmdb_owner', '--dbname', 'tmdb', '-c', $Sql
    ))
    if ([string]::IsNullOrWhiteSpace($output)) {
        throw 'PostgreSQL returned no JSON during the media-asset verification.'
    }
    try {
        return ($output | ConvertFrom-Json)
    }
    catch {
        throw 'PostgreSQL returned invalid JSON during the media-asset verification.'
    }
}

function Get-StaticStatus {
    param(
        [Parameter(Mandatory)][System.Net.Http.HttpClient]$Client,
        [Parameter(Mandatory)][string]$Url
    )

    $response = $null
    try {
        $response = $Client.GetAsync($Url).GetAwaiter().GetResult()
        $bytes = $response.Content.ReadAsByteArrayAsync().GetAwaiter().GetResult()
        return [ordered]@{ status = [int]$response.StatusCode; bytes = $bytes.Length }
    }
    finally {
        if ($null -ne $response) { $response.Dispose() }
    }
}

$password = $null
$client = [System.Net.Http.HttpClient]::new()
$client.Timeout = [TimeSpan]::FromSeconds(15)
try {
    $script:verificationStage = 'read database password'
    $password = Invoke-DockerChecked -Arguments ($composeArgs + @('exec', '-T', 'postgres', 'printenv', 'POSTGRES_PASSWORD'))
    if ([string]::IsNullOrWhiteSpace($password)) {
        throw 'The disposable PostgreSQL password is unavailable.'
    }

    $assetSql = @'
WITH categorized AS (
    SELECT CASE
        WHEN title_id IS NOT NULL THEN 'title'
        WHEN season_id IS NOT NULL THEN 'season'
        WHEN episode_id IS NOT NULL THEN 'episode'
        WHEN person_id IS NOT NULL THEN 'person'
        WHEN company_id IS NOT NULL THEN 'company'
        WHEN network_id IS NOT NULL THEN 'network'
        WHEN collection_id IS NOT NULL THEN 'collection'
        ELSE 'other'
    END AS category,
    storage_path
    FROM assets.image_assets
    WHERE status = 'ready'
), ranked AS (
    SELECT category, storage_path,
           row_number() OVER (PARTITION BY category ORDER BY storage_path) AS row_number
    FROM categorized
)
SELECT COALESCE(json_agg(json_build_object('category', category, 'storage_path', storage_path) ORDER BY category), '[]'::json)::text
FROM ranked
WHERE row_number = 1;
'@
    $script:verificationStage = 'query representative assets'
    $candidates = @(Invoke-PostgresJson -Password $password -Sql $assetSql)
    $requiredCategories = @('title', 'season', 'episode', 'person', 'company', 'network', 'collection')
    $missingCategories = @(foreach ($requiredCategory in $requiredCategories) {
        if (@($candidates | Where-Object { $_.category -eq $requiredCategory }).Count -eq 0) {
            $requiredCategory
        }
    })
    if ($missingCategories.Count -gt 0) {
        throw "Required media owner categories are missing: $($missingCategories -join ', ')."
    }

    $checks = [System.Collections.Generic.List[object]]::new()
    foreach ($candidate in $candidates) {
        $category = [string]$candidate.category
        $storagePath = [string]$candidate.storage_path
        if ($storagePath -notmatch '^[a-zA-Z0-9][a-zA-Z0-9._/-]*$' -or $storagePath.Contains('..')) {
            throw "Database returned an unsafe media path for $category."
        }
        $http = Get-StaticStatus -Client $client -Url "http://127.0.0.1:$ImagePort/media/$storagePath"
        $script:verificationStage = "verify $category file"
        $fileExists = (Invoke-DockerChecked -Arguments ($composeArgs + @(
            'exec', '-T', 'media', 'sh', '-ec', "test -f '/media/$storagePath'"
        ))) -eq ''
        $checks.Add([ordered]@{
            category = $category
            storage_path = $storagePath
            http_status = $http.status
            response_bytes = $http.bytes
            file_exists_under_media = $fileExists
            passed = [bool]($http.status -eq 200 -and $http.bytes -gt 0 -and $fileExists)
        })
    }

    $integritySql = @'
SELECT json_build_object(
    'dead_letter_image_jobs', (
        SELECT count(*) FROM ops.jobs WHERE job_type = 'image.download' AND status = 'dead_letter'
    ),
    'worker_ids', (
        SELECT COALESCE(json_agg(worker_id ORDER BY worker_id), '[]'::json)
        FROM (
            SELECT DISTINCT event.worker_id
            FROM ops.job_events AS event
            JOIN ops.jobs AS job ON job.id = event.job_id
            WHERE job.job_type = 'image.download'
              AND event.worker_id LIKE 'tmdb-stress-media-%'
        ) workers
    ),
    'shared_source_owner_groups', (
        SELECT count(*) FROM (
            SELECT source, source_key
            FROM assets.image_assets
            GROUP BY source, source_key
            HAVING count(DISTINCT owner_type || ':' || owner_id) > 1
        ) groups
    )
)::text;
'@
    $script:verificationStage = 'query media integrity'
    $integrity = Invoke-PostgresJson -Password $password -Sql $integritySql
    $workerIds = @($integrity.worker_ids)
    $result = [ordered]@{
        checked_at_utc = [DateTime]::UtcNow.ToString('O')
        expected_worker_count = $ExpectedWorkers
        observed_worker_ids = $workerIds
        shared_source_owner_groups = [int]$integrity.shared_source_owner_groups
        dead_letter_image_jobs = [int]$integrity.dead_letter_image_jobs
        checks = @($checks)
        passed = [bool](
            @($checks | Where-Object { -not $_.passed }).Count -eq 0 -and
            $workerIds.Count -eq $ExpectedWorkers -and
            [int]$integrity.shared_source_owner_groups -gt 0 -and
            [int]$integrity.dead_letter_image_jobs -eq 0
        )
    }
    [IO.File]::WriteAllText($resultFile, (($result | ConvertTo-Json -Depth 8) + "`n"), [Text.UTF8Encoding]::new($false))
    Write-Output ($result | ConvertTo-Json -Depth 8)
    Write-Output "Media-asset verification artifact: $resultFile"
    if (-not $result.passed) { exit 2 }
}
finally {
    $client.Dispose()
    $password = $null
}
