[CmdletBinding()]
param(
    [string]$ProjectName = 'tmdb_stress_test',
    [string]$TmdbReadToken = $env:TMDB_STRESS_READ_TOKEN,
    [string]$SecretsFile
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $PSScriptRoot
. (Join-Path $repoRoot 'scripts/stress-secrets.ps1')
if ([string]::IsNullOrWhiteSpace($SecretsFile)) {
    $SecretsFile = Join-Path $repoRoot 'secrets.txt'
}
elseif (-not (Test-Path -LiteralPath $SecretsFile -PathType Leaf)) {
    throw "Local stress secrets file is missing: $SecretsFile"
}
$localSecrets = Read-StressSecrets -Path $SecretsFile
$TmdbReadToken = Resolve-StressSecret `
    -Secrets $localSecrets -Name 'TMDB_STRESS_READ_TOKEN' -ExplicitValue $TmdbReadToken

if ([string]::IsNullOrWhiteSpace($TmdbReadToken)) {
    throw 'Provide the token through -TmdbReadToken, TMDB_STRESS_READ_TOKEN, or the ignored secrets.txt file.'
}

$runtimeRoot = Join-Path (Join-Path $repoRoot '.stress-runtime') $ProjectName
$envFile = Join-Path $runtimeRoot 'compose.env'
if (-not (Test-Path -LiteralPath $envFile -PathType Leaf)) {
    throw "Stress runtime environment is missing: $envFile. Run stress-bootstrap.ps1 first."
}

try {
    $lines = [System.Collections.Generic.List[string]]::new()
    $lines.AddRange([IO.File]::ReadAllLines($envFile, [Text.UTF8Encoding]::new($false)))
    $index = -1
    for ($i = 0; $i -lt $lines.Count; $i++) {
        if ($lines[$i].StartsWith('TMDB_READ_ACCESS_TOKEN=', [StringComparison]::Ordinal)) {
            $index = $i
            break
        }
    }
    if ($index -lt 0) { throw 'TMDB_READ_ACCESS_TOKEN is missing from the stress environment.' }
    $lines[$index] = "TMDB_READ_ACCESS_TOKEN=$($TmdbReadToken.Trim())"
    [IO.File]::WriteAllLines($envFile, $lines, [Text.UTF8Encoding]::new($false))
    Write-Output "Updated the isolated runtime token in $envFile (value not displayed)."
    Write-Output 'Recreate the worker container so Docker reloads the env_file.'
}
finally {
    $TmdbReadToken = $null
    $localSecrets = $null
    $env:TMDB_STRESS_READ_TOKEN = $null
}
