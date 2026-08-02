[CmdletBinding()]
param(
    [string]$SecretsFile
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
Add-Type -AssemblyName System.Net.Http

$repoRoot = Split-Path -Parent $PSScriptRoot
. (Join-Path $repoRoot 'scripts/stress-secrets.ps1')
if ([string]::IsNullOrWhiteSpace($SecretsFile)) {
    $SecretsFile = Join-Path $repoRoot 'secrets.txt'
}
elseif (-not (Test-Path -LiteralPath $SecretsFile -PathType Leaf)) {
    throw "Local stress secrets file is missing: $SecretsFile"
}

$secrets = Read-StressSecrets -Path $SecretsFile
$readToken = Resolve-StressSecret `
    -Secrets $secrets -Name 'TMDB_STRESS_READ_TOKEN' -ExplicitValue $env:TMDB_STRESS_READ_TOKEN
$v3ApiKey = Resolve-StressSecret `
    -Secrets $secrets -Name 'TMDB_STRESS_API_KEY' -ExplicitValue $env:TMDB_STRESS_API_KEY
if ([string]::IsNullOrWhiteSpace($readToken) -or [string]::IsNullOrWhiteSpace($v3ApiKey)) {
    throw 'The ignored secrets file must provide both TMDB_STRESS_READ_TOKEN and TMDB_STRESS_API_KEY.'
}

function Get-TmdbStatus {
    param(
        [Parameter(Mandatory)][System.Net.Http.HttpClient]$Client,
        [Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][ValidateSet('bearer', 'v3')][string]$Method,
        [Parameter(Mandatory)][string]$ReadToken,
        [Parameter(Mandatory)][string]$ApiKey
    )

    $request = $null
    $response = $null
    try {
        if ($Method -eq 'bearer') {
            $request = [System.Net.Http.HttpRequestMessage]::new(
                [System.Net.Http.HttpMethod]::Get,
                "https://api.themoviedb.org/3/$Path"
            )
            $request.Headers.Authorization = [System.Net.Http.Headers.AuthenticationHeaderValue]::new('Bearer', $ReadToken)
        }
        else {
            $uri = [System.UriBuilder]::new('https', 'api.themoviedb.org', 443, "/3/$Path")
            $uri.Query = 'api_key=' + [System.Uri]::EscapeDataString($ApiKey)
            $request = [System.Net.Http.HttpRequestMessage]::new([System.Net.Http.HttpMethod]::Get, $uri.Uri)
        }
        $response = $Client.SendAsync($request).GetAwaiter().GetResult()
        return [int]$response.StatusCode
    }
    finally {
        if ($null -ne $response) { $response.Dispose() }
        if ($null -ne $request) { $request.Dispose() }
    }
}

$client = [System.Net.Http.HttpClient]::new()
$client.Timeout = [TimeSpan]::FromSeconds(30)
try {
    $result = [ordered]@{
        checked_at_utc = [DateTime]::UtcNow.ToString('O')
        bearer_read_token = [ordered]@{
            configuration_status = Get-TmdbStatus -Client $client -Path 'configuration' -Method bearer -ReadToken $readToken -ApiKey $v3ApiKey
            movie_detail_status = Get-TmdbStatus -Client $client -Path 'movie/550' -Method bearer -ReadToken $readToken -ApiKey $v3ApiKey
        }
        v3_api_key = [ordered]@{
            configuration_status = Get-TmdbStatus -Client $client -Path 'configuration' -Method v3 -ReadToken $readToken -ApiKey $v3ApiKey
            movie_detail_status = Get-TmdbStatus -Client $client -Path 'movie/550' -Method v3 -ReadToken $readToken -ApiKey $v3ApiKey
        }
    }
    $result.passed = [bool](
        $result.bearer_read_token.configuration_status -eq 200 -and
        $result.bearer_read_token.movie_detail_status -eq 200 -and
        $result.v3_api_key.configuration_status -eq 200 -and
        $result.v3_api_key.movie_detail_status -eq 200
    )
    Write-Output ($result | ConvertTo-Json -Depth 5)
    if (-not $result.passed) {
        exit 2
    }
}
finally {
    $client.Dispose()
    $readToken = $null
    $v3ApiKey = $null
    $secrets = $null
    Remove-Item Env:TMDB_STRESS_READ_TOKEN -ErrorAction SilentlyContinue
    Remove-Item Env:TMDB_STRESS_API_KEY -ErrorAction SilentlyContinue
}
