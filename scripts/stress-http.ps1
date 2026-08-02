[CmdletBinding()]
param(
    [string]$ProjectName = 'tmdb_stress_test',
    [int]$Concurrency = 100,
    [ValidateRange(1, 10000)][int]$RequestsPerWorker = 50,
    [int]$ApiPort = 18080,
    [int]$ImagePort = 18090,
    [int]$TimeoutMilliseconds = 10000
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

# Windows PowerShell 5.1 does not load System.Net.Http until explicitly
# requested; the runspace workers use HttpClient for high-concurrency traffic.
Add-Type -AssemblyName System.Net.Http

$repoRoot = Split-Path -Parent $PSScriptRoot
$runtimeRoot = Join-Path (Join-Path $repoRoot '.stress-runtime') $ProjectName
$resultRoot = Join-Path $runtimeRoot 'results'
New-Item -ItemType Directory -Force -Path $resultRoot | Out-Null
$timestamp = [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ')
$resultFile = Join-Path $resultRoot "http-$timestamp.json"
$baseUrl = "http://127.0.0.1:$ApiPort"
$imageUrl = "http://127.0.0.1:$ImagePort"

if ($Concurrency -lt 1 -or $Concurrency -gt 500) { throw 'Concurrency must be between 1 and 500.' }

$paths = @(
    '/health/live',
    '/health/ready',
    '/movies?limit=20',
    '/movies/top-rated?limit=20',
    '/movies/recent?limit=20',
    '/movies?genreId=900000002&language=en&runtimeMin=40&runtimeMax=120&personId=900000002&companyId=900000002&limit=20',
    '/tv?limit=20',
    '/tv/top-rated?limit=20',
    '/tv/recent?limit=20',
    '/anime?limit=20',
    '/anime?q=One%20Piece&limit=20',
    '/anime/top-rated?limit=20',
    '/anime/recent?limit=20',
    '/search?q=Caf%C3%A9&limit=20',
    '/search?q=One%20Piece&limit=20',
    '/genres?limit=20',
    '/languages?limit=20',
    '/keywords?q=anime&limit=20',
    '/tags?q=stress&limit=20',
    '/people?q=Beyonc%C3%A9&limit=20',
    '/companies?q=Stress&limit=20',
    '/networks?q=Stress&limit=20',
    '/collections?limit=20',
    '/movies/900000001',
    '/tv/900000002',
    '/anime/movie/900667',
    '/anime/tv/37854',
    '/movies/900000001/credits',
    '/tv/900000002/credits',
    '/movies/900000001/images',
    '/tv/900000002/images',
    '/anime/movie/900667/images',
    '/anime/tv/37854/images',
    '/tv/900000002/seasons',
    '/tv/900000002/seasons/1/episodes',
    '/tv/900000002/seasons/1/episodes/1'
)

$workerScript = {
    param($WorkerBaseUrl, $WorkerPaths, $WorkerRequests, $WorkerId, $WorkerTimeout)
    $client = [System.Net.Http.HttpClient]::new()
    $client.Timeout = [TimeSpan]::FromMilliseconds($WorkerTimeout)
    $rows = [System.Collections.Generic.List[object]]::new()
    try {
        for ($requestNumber = 0; $requestNumber -lt $WorkerRequests; $requestNumber++) {
            $path = $WorkerPaths[($requestNumber + $WorkerId) % $WorkerPaths.Count]
            $uri = "$WorkerBaseUrl$path"
            $watch = [System.Diagnostics.Stopwatch]::StartNew()
            $status = 0
            $bytes = 0
            $requestError = $null
            try {
                $response = $client.GetAsync($uri).GetAwaiter().GetResult()
                $body = $response.Content.ReadAsByteArrayAsync().GetAwaiter().GetResult()
                $status = [int]$response.StatusCode
                $bytes = $body.Length
                $response.Dispose()
            }
            catch {
                $requestError = $_.Exception.GetType().Name
            }
            $watch.Stop()
            [void]$rows.Add([pscustomobject]@{
                worker = $WorkerId
                request = $requestNumber
                path = $path
                status = $status
                latency_ms = [math]::Round($watch.Elapsed.TotalMilliseconds, 3)
                response_bytes = $bytes
                error = $requestError
            })
        }
    }
    finally {
        $client.Dispose()
    }
    foreach ($row in $rows) { Write-Output $row }
}

$pool = [System.Management.Automation.Runspaces.RunspaceFactory]::CreateRunspacePool(1, $Concurrency)
$pool.Open()
$pending = [System.Collections.Generic.List[object]]::new()
$overall = [System.Diagnostics.Stopwatch]::StartNew()
try {
    for ($workerId = 0; $workerId -lt $Concurrency; $workerId++) {
        $ps = [powershell]::Create()
        $ps.RunspacePool = $pool
        [void]$ps.AddScript($workerScript.ToString()).AddArgument($baseUrl).AddArgument($paths).AddArgument($RequestsPerWorker).AddArgument($workerId).AddArgument($TimeoutMilliseconds)
        $handle = $ps.BeginInvoke()
        $pending.Add([pscustomobject]@{ PowerShell = $ps; Handle = $handle })
    }

    $rows = [System.Collections.Generic.List[object]]::new()
    foreach ($job in $pending) {
        try {
            foreach ($row in $job.PowerShell.EndInvoke($job.Handle)) { [void]$rows.Add($row) }
            if ($job.PowerShell.Streams.Error.Count -gt 0) {
                foreach ($workerError in $job.PowerShell.Streams.Error) {
                    [void]$rows.Add([pscustomobject]@{
                        worker = -1
                        request = -1
                        path = '<worker-startup>'
                        status = 0
                        latency_ms = 0
                        response_bytes = 0
                        error = $workerError.Exception.GetType().Name
                    })
                }
            }
        }
        finally {
            $job.PowerShell.Dispose()
        }
    }
}
finally {
    $overall.Stop()
    $pool.Close()
    $pool.Dispose()
}

function Get-Percentile {
    param([double[]]$Values, [double]$Quantile)
    if ($Values.Count -eq 0) { return $null }
    $ordered = @($Values | Sort-Object)
    $index = [math]::Ceiling($Quantile * $ordered.Count) - 1
    if ($index -lt 0) { $index = 0 }
    if ($index -ge $ordered.Count) { $index = $ordered.Count - 1 }
    return [math]::Round([double]$ordered[$index], 3)
}

$allRows = @($rows)
$latencies = @($allRows | ForEach-Object { [double]$_.latency_ms })
$successful = @($allRows | Where-Object { $_.status -ge 200 -and $_.status -lt 300 })
$failed = @($allRows | Where-Object { $_.status -lt 200 -or $_.status -ge 300 -or $null -ne $_.error })
$elapsedSeconds = [math]::Max($overall.Elapsed.TotalSeconds, 0.001)
$summary = [ordered]@{
    started_at_utc = $timestamp
    base_url = $baseUrl
    concurrency = $Concurrency
    requests_per_worker = $RequestsPerWorker
    total_requests = $allRows.Count
    successful_requests = $successful.Count
    failed_requests = $failed.Count
    requests_per_second = [math]::Round($allRows.Count / $elapsedSeconds, 3)
    elapsed_seconds = [math]::Round($elapsedSeconds, 3)
    latency_ms = [ordered]@{
        p50 = Get-Percentile -Values $latencies -Quantile 0.50
        p95 = Get-Percentile -Values $latencies -Quantile 0.95
        p99 = Get-Percentile -Values $latencies -Quantile 0.99
        max = if ($latencies.Count) { [math]::Round(($latencies | Measure-Object -Maximum).Maximum, 3) } else { $null }
    }
    status_counts = @($allRows | Group-Object status | Sort-Object Name | ForEach-Object {
        [ordered]@{ status = $_.Name; count = $_.Count }
    })
    error_counts = @($allRows | Where-Object { $null -ne $_.error } | Group-Object error | Sort-Object Name | ForEach-Object {
        [ordered]@{ error = $_.Name; count = $_.Count }
    })
}

function Invoke-JsonGet {
    param([Parameter(Mandatory)][string]$Url)
    $client = [System.Net.Http.HttpClient]::new()
    try {
        $response = $client.GetAsync($Url).GetAwaiter().GetResult()
        $body = $response.Content.ReadAsStringAsync().GetAwaiter().GetResult()
        if (-not $response.IsSuccessStatusCode) {
            throw "HTTP $([int]$response.StatusCode) from $Url"
        }
        return ($body | ConvertFrom-Json)
    }
    finally { $client.Dispose() }
}

function Assert-HttpSuccess {
    param([Parameter(Mandatory)][string]$Url)
    $client = [System.Net.Http.HttpClient]::new()
    try {
        $response = $client.GetAsync($Url).GetAwaiter().GetResult()
        $status = [int]$response.StatusCode
        $response.Content.ReadAsByteArrayAsync().GetAwaiter().GetResult() | Out-Null
        if (-not $response.IsSuccessStatusCode) { throw "HTTP $status from $Url" }
        return $status
    }
    finally { $client.Dispose() }
}

$semantic = [System.Collections.Generic.List[object]]::new()
function Add-SemanticCheck {
    param([string]$Name, [bool]$Passed, [string]$Detail)
    $semantic.Add([ordered]@{ name = $Name; passed = $Passed; detail = $Detail })
}

try {
    $movies = Invoke-JsonGet -Url "$baseUrl/movies?limit=100"
    $movieAnime = @($movies.data | Where-Object { $_.isAnime })
    Add-SemanticCheck 'ordinary_routes_exclude_anime' ($movieAnime.Count -eq 0) "returned=$($movies.data.Count), anime=$($movieAnime.Count)"

    $anime = Invoke-JsonGet -Url "$baseUrl/anime?q=One%20Piece&limit=100"
    $animeRows = @($anime.data)
    $nonAnime = @($animeRows | Where-Object { -not $_.isAnime })
    Add-SemanticCheck 'anime_route_is_anime_only' ($animeRows.Count -gt 0 -and $nonAnime.Count -eq 0) "returned=$($animeRows.Count), non_anime=$($nonAnime.Count)"

    $accent = Invoke-JsonGet -Url "$baseUrl/search?q=Caf%C3%A9&limit=20"
    Add-SemanticCheck 'unaccent_search_returns_rows' (@($accent.data).Count -gt 0) "returned=$(@($accent.data).Count)"

    $filtered = Invoke-JsonGet -Url "$baseUrl/movies?genreId=900000002&language=en&runtimeMin=40&runtimeMax=120&personId=900000002&companyId=900000002&limit=20"
    Add-SemanticCheck 'multi_facet_filter_returns_rows' (@($filtered.data).Count -gt 0) "returned=$(@($filtered.data).Count)"

    $animeMovieImages = Invoke-JsonGet -Url "$baseUrl/anime/movie/900667/images"
    $animeTvImages = Invoke-JsonGet -Url "$baseUrl/anime/tv/37854/images"
    $animeImageRows = @($animeMovieImages.data) + @($animeTvImages.data)
    $localAnimeImages = @($animeImageRows | Where-Object { [string]$_.url -like "$imageUrl/media/anime/*" })
    Add-SemanticCheck 'anime_image_metadata_routes_return_local_urls' ($animeImageRows.Count -ge 2 -and $localAnimeImages.Count -eq $animeImageRows.Count) "returned=$($animeImageRows.Count), local=$($localAnimeImages.Count)"

    $imageHealth = Assert-HttpSuccess -Url "$imageUrl/healthz"
    Add-SemanticCheck 'static_image_server_health' $true "HTTP $imageHealth"
}
catch {
    Add-SemanticCheck 'semantic_checks' $false $_.Exception.Message
}

$summary.semantic_checks = @($semantic)
$summary.failed_request_samples = @($failed | Select-Object -First 20)
$artifact = [ordered]@{ summary = $summary; requests = $allRows }
[System.IO.File]::WriteAllText($resultFile, (($artifact | ConvertTo-Json -Depth 8) + "`n"), [System.Text.UTF8Encoding]::new($false))
Write-Output (($summary | ConvertTo-Json -Depth 8))
Write-Output "HTTP stress artifact: $resultFile"

if ($summary.failed_requests -gt 0 -or @($semantic | Where-Object { -not $_.passed }).Count -gt 0) {
    exit 2
}
