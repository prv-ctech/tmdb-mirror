[CmdletBinding()]
param(
    [string]$ProjectName = 'tmdb_stress_test',
    [datetime]$Date = [DateTime]::UtcNow.Date,
    [ValidateRange(0, 14)][int]$MaxLookbackDays = 7,
    [ValidateRange(0, 100000)][int]$QueueLimit = 500
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
Add-Type -AssemblyName System.Net.Http

$repoRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $repoRoot 'deploy/compose.stress.yaml'
$runtimeRoot = Join-Path (Join-Path $repoRoot '.stress-runtime') $ProjectName
$envFile = Join-Path $runtimeRoot 'compose.env'
$exportRoot = Join-Path $runtimeRoot 'exports'
$resultRoot = Join-Path $runtimeRoot 'results'
New-Item -ItemType Directory -Force -Path $exportRoot, $resultRoot | Out-Null
$stamp = [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ')
$composeArgs = @('compose', '--env-file', $envFile, '--project-name', $ProjectName, '--file', $composeFile)
$dateWasExplicit = $PSBoundParameters.ContainsKey('Date')

if (-not (Test-Path -LiteralPath $envFile -PathType Leaf)) {
    throw "Runtime environment is missing: $envFile. Run stress-bootstrap.ps1 first."
}

function Download-Export {
    param(
        [Parameter(Mandatory)][string]$MediaType,
        [Parameter(Mandatory)][datetime]$ExportDate
    )
    # TMDB's public export contract is MM_DD_YYYY (not ISO order).
    $dateText = $ExportDate.ToUniversalTime().ToString('MM_dd_yyyy')
    $fileName = if ($MediaType -eq 'movie') { "movie_ids_$dateText.json.gz" } else { "tv_series_ids_$dateText.json.gz" }
    $url = "https://files.tmdb.org/p/exports/$fileName"
    $destination = Join-Path $exportRoot $fileName
    $client = [System.Net.Http.HttpClient]::new()
    try {
        $response = $client.GetAsync($url, [System.Net.Http.HttpCompletionOption]::ResponseHeadersRead).GetAwaiter().GetResult()
        if (-not $response.IsSuccessStatusCode) {
            $status = [int]$response.StatusCode
            $response.Dispose()
            if ($status -in @(403, 404)) {
                return [pscustomobject]@{
                    available = $false
                    media_type = $MediaType
                    date = $dateText
                    status = $status
                }
            }
            throw "TMDB export returned HTTP $status for $fileName"
        }
        $inputStream = $response.Content.ReadAsStreamAsync().GetAwaiter().GetResult()
        $outputStream = [System.IO.File]::Open($destination, [System.IO.FileMode]::Create, [System.IO.FileAccess]::Write, [System.IO.FileShare]::None)
        try { $inputStream.CopyTo($outputStream) }
        finally {
            $outputStream.Dispose()
            $inputStream.Dispose()
            $response.Dispose()
        }
    }
    finally { $client.Dispose() }
    [pscustomobject]@{
        available = $true
        media_type = $MediaType
        date = $dateText
        url = $url
        host_path = $destination
        file_name = $fileName
    }
}

function Copy-ToWorkerVolume {
    param([Parameter(Mandatory)][string]$HostPath, [Parameter(Mandatory)][string]$FileName)
    $previous = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
    $containerOutput = @(& docker @($composeArgs + @('ps', '-q', 'worker')) 2>&1)
        $exitCode = $LASTEXITCODE
    }
    finally { $ErrorActionPreference = $previous }
    $containerId = ([string]::Join("`n", $containerOutput)).Trim()
    if ($exitCode -ne 0 -or [string]::IsNullOrWhiteSpace($containerId)) {
        throw 'The consolidated worker container is not running; cannot place the export in the shared config volume.'
    }
    # The stress /config volume is intentionally an empty tmpfs on a fresh
    # run.  Create the fixed raw-export directory inside the container before
    # docker cp resolves its destination; this keeps all scan input below
    # /config without relying on a host-path convention.
    & docker @($composeArgs + @('exec', '-T', 'worker', 'mkdir', '-p', '/config/raw')) 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Could not create /config/raw in the consolidated worker volume.' }
    $destination = "/config/raw/$FileName"
    & docker cp $HostPath "$containerId`:$destination" 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Could not copy export into the stress work volume: $FileName" }
    return $destination
}

function Invoke-ScanCommand {
    param(
        [Parameter(Mandatory)][string]$MediaType,
        [Parameter(Mandatory)][string]$ContainerPath
    )
    $arguments = $composeArgs + @(
        'run', '--rm', '--no-deps', '--entrypoint', '/usr/local/bin/tmdb-admin', 'worker',
        'scan-export', '--path', $ContainerPath, '--media-type', $MediaType
    )
    if ($QueueLimit -gt 0) { $arguments += @('--queue-limit', $QueueLimit.ToString()) }
    $previous = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $output = @(& docker @arguments 2>&1)
        $exitCode = $LASTEXITCODE
    }
    finally { $ErrorActionPreference = $previous }
    if ($exitCode -ne 0) {
        throw "TMDB export validation/queue failed for $MediaType.`n$([string]::Join("`n", $output))"
    }
    $jsonLine = @($output | Where-Object { $_.ToString().Trim().StartsWith('{') } | Select-Object -Last 1)
    if ($jsonLine.Count -ne 1) {
        throw "TMDB export command returned no machine-readable result for $MediaType."
    }
    return ($jsonLine[0].ToString() | ConvertFrom-Json)
}

$downloads = @{}
$selectedDate = $null
$attemptedDates = [System.Collections.Generic.List[object]]::new()
for ($offset = 0; $offset -le $MaxLookbackDays; $offset++) {
    $candidateDate = $Date.Date.AddDays(-$offset)
    $candidateDownloads = @{}
    $unavailable = [System.Collections.Generic.List[object]]::new()
    foreach ($mediaType in @('movie', 'tv')) {
        $download = Download-Export -MediaType $mediaType -ExportDate $candidateDate
        if ($download.available) {
            $candidateDownloads[$mediaType] = $download
        }
        else {
            $unavailable.Add($download)
        }
    }
    $attemptedDates.Add([ordered]@{
        date = $candidateDate.ToUniversalTime().ToString('yyyy-MM-dd')
        unavailable = @($unavailable | ForEach-Object { "$($_.media_type):$($_.status)" })
    })
    if ($unavailable.Count -eq 0) {
        $downloads = $candidateDownloads
        $selectedDate = $candidateDate
        break
    }
    if ($dateWasExplicit) {
        throw "TMDB exports for the requested date are unavailable: $($unavailable.media_type -join ', ')."
    }
}
if ($null -eq $selectedDate) {
    throw "TMDB did not publish matching movie and TV exports within $MaxLookbackDays day(s)."
}

$results = [System.Collections.Generic.List[object]]::new()
foreach ($mediaType in @('movie', 'tv')) {
    $download = $downloads[$mediaType]
    $containerPath = Copy-ToWorkerVolume -HostPath $download.host_path -FileName $download.file_name
    $scan = Invoke-ScanCommand -MediaType $mediaType -ContainerPath $containerPath
    $results.Add([ordered]@{
        media_type = $mediaType
        date = $download.date
        url = $download.url
        compressed_bytes = (Get-Item -LiteralPath $download.host_path).Length
        full_records = $scan.full_records
        queue_limit = $scan.queue_limit
        queued = $scan.queued
        duplicates = $scan.duplicates
    })
}

$artifact = [ordered]@{
    started_at_utc = $stamp
    requested_date_utc = $Date.ToUniversalTime().ToString('yyyy-MM-dd')
    selected_date_utc = $selectedDate.ToUniversalTime().ToString('yyyy-MM-dd')
    attempted_dates = @($attemptedDates)
    results = @($results)
}
$resultFile = Join-Path $resultRoot "tmdb-scan-$stamp.json"
[System.IO.File]::WriteAllText($resultFile, (($artifact | ConvertTo-Json -Depth 8) + "`n"), [System.Text.UTF8Encoding]::new($false))
Write-Output (($artifact | ConvertTo-Json -Depth 8))
Write-Output "TMDB scan artifact: $resultFile"
