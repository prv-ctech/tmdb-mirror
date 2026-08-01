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
}
finally {
    $TmdbReadToken = $null
    $env:TMDB_STRESS_READ_TOKEN = $null
}
