[CmdletBinding()]
param(
    [string]$ProjectName = 'tmdb_stress_test',
    [string]$SecretsFile
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $PSScriptRoot
. (Join-Path $repoRoot 'scripts/stress-secrets.ps1')
$composeFile = Join-Path $repoRoot 'deploy/compose.stress.yaml'
$runtimeRoot = Join-Path (Join-Path $repoRoot '.stress-runtime') $ProjectName
$envFile = Join-Path $runtimeRoot 'compose.env'
$resultRoot = Join-Path $runtimeRoot 'results'
$stamp = [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ')
$resultFile = Join-Path $resultRoot "trawl-$stamp.json"

if (-not (Test-Path -LiteralPath $envFile -PathType Leaf)) {
    throw "Runtime environment is missing: $envFile. Run stress-bootstrap.ps1 first."
}
if ([string]::IsNullOrWhiteSpace($SecretsFile)) {
    $SecretsFile = Join-Path $repoRoot 'secrets.txt'
}
elseif (-not (Test-Path -LiteralPath $SecretsFile -PathType Leaf)) {
    throw "Local stress secrets file is missing: $SecretsFile"
}
New-Item -ItemType Directory -Force -Path $resultRoot | Out-Null

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
        throw 'Docker failed during the Trawl probe.'
    }
    return [string]::Join("`n", $output).Trim()
}

$secrets = $null
$trawlBaseUrl = $null
$databasePassword = $null
try {
    $secrets = Read-StressSecrets -Path $SecretsFile
    $trawlBaseUrl = Resolve-StressSecret -Secrets $secrets -Name 'TMDB_STRESS_TRAWL_BASE_URL'
    if ([string]::IsNullOrWhiteSpace($trawlBaseUrl)) {
        throw 'The ignored secrets file must provide TMDB_STRESS_TRAWL_BASE_URL.'
    }
    $trawlBaseUrl = $trawlBaseUrl.TrimEnd('/')

    $configuredUrl = Invoke-DockerChecked -Arguments ($composeArgs + @(
        'exec', '-T', 'media', 'printenv', 'TMDB_TRAWL_BASE_URL'
    ))
    if ($configuredUrl.TrimEnd('/') -ne $trawlBaseUrl) {
        throw 'The media worker does not have the requested Trawl fallback URL.'
    }

    $databasePassword = Invoke-DockerChecked -Arguments ($composeArgs + @(
        'exec', '-T', 'postgres', 'printenv', 'POSTGRES_PASSWORD'
    ))
    if ([string]::IsNullOrWhiteSpace($databasePassword)) {
        throw 'The disposable PostgreSQL password is unavailable.'
    }
    $sourceKey = Invoke-DockerChecked -Arguments ($composeArgs + @(
        'exec', '-T', '-e', "PGPASSWORD=$databasePassword", 'postgres', 'psql', '-X', '-At',
        '--username', 'tmdb_owner', '--dbname', 'tmdb', '-c',
        "SELECT source_key FROM assets.image_assets WHERE status = 'ready' AND source = 'tmdb' ORDER BY id LIMIT 1;"
    ))
    if ($sourceKey -notmatch '^/[A-Za-z0-9._-]+$') {
        throw 'No safe TMDB image source key is available for the Trawl probe.'
    }

    $response = Invoke-WebRequest -UseBasicParsing -Method Post -Uri "$trawlBaseUrl/scrape" `
        -ContentType 'application/x-www-form-urlencoded' `
        -Body @{ url = "https://image.tmdb.org/t/p/w185$sourceKey"; maxTimeout = '20000' } `
        -TimeoutSec 30
    $envelope = $response.Content | ConvertFrom-Json
    $upstreamStatus = if ($null -ne $envelope.PSObject.Properties['statusCode']) {
        [int]$envelope.statusCode
    }
    elseif ($null -ne $envelope.PSObject.Properties['status']) {
        [int]$envelope.status
    }
    else {
        0
    }
    $hasBody = $null -ne $envelope.body -and @($envelope.body).Count -gt 0
    $result = [ordered]@{
        checked_at_utc = [DateTime]::UtcNow.ToString('O')
        media_worker_configuration_matches = $true
        trawl_http_status = [int]$response.StatusCode
        upstream_http_status = $upstreamStatus
        content_type_present = -not [string]::IsNullOrWhiteSpace([string]$envelope.contentType)
        binary_body_present = $hasBody
        passed = [bool](
            [int]$response.StatusCode -eq 200 -and
            $upstreamStatus -eq 200 -and
            $hasBody
        )
    }
    [IO.File]::WriteAllText($resultFile, (($result | ConvertTo-Json -Depth 5) + "`n"), [Text.UTF8Encoding]::new($false))
    Write-Output ($result | ConvertTo-Json -Depth 5)
    Write-Output "Trawl probe artifact: $resultFile"
    if (-not $result.passed) { exit 2 }
}
finally {
    $databasePassword = $null
    $trawlBaseUrl = $null
    $secrets = $null
}
