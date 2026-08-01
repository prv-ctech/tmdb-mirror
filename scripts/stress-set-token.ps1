[CmdletBinding()]
param(
    [string]$ProjectName = 'tmdb_stress_test',
    [string]$TmdbReadToken = $env:TMDB_STRESS_READ_TOKEN
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

if ([string]::IsNullOrWhiteSpace($TmdbReadToken)) {
    throw 'Provide the token through -TmdbReadToken or TMDB_STRESS_READ_TOKEN.'
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$runtimeRoot = Join-Path (Join-Path $repoRoot '.stress-runtime') $ProjectName
$secretRoot = Join-Path $runtimeRoot 'secrets'
$tokenPath = Join-Path $secretRoot 'tmdb_read_access_token'
if (-not (Test-Path -LiteralPath $secretRoot -PathType Container)) {
    throw "Stress runtime secrets are missing: $secretRoot. Run stress-bootstrap.ps1 first."
}

try {
    [IO.File]::WriteAllText($tokenPath, $TmdbReadToken.Trim() + "`n", [Text.UTF8Encoding]::new($false))
    Write-Output "Updated the isolated runtime TMDB token at $tokenPath (value intentionally not displayed)."
}
finally {
    $TmdbReadToken = $null
    $env:TMDB_STRESS_READ_TOKEN = $null
}
