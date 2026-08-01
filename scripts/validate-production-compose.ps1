[CmdletBinding()]
param(
    [string]$EnvFile = '',
    [string]$ComposeFile = ''
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

# Resolve defaults after PowerShell has initialized $PSScriptRoot. Parameter
# default expressions are evaluated before that automatic variable is usable
# when the script is invoked from another working directory.
if ([string]::IsNullOrWhiteSpace($EnvFile)) {
    $EnvFile = Join-Path $PSScriptRoot '..\deploy\env.production'
}
if ([string]::IsNullOrWhiteSpace($ComposeFile)) {
    $ComposeFile = Join-Path $PSScriptRoot '..\deploy\compose.compact.yaml'
}

function Resolve-RequiredFile {
    param([Parameter(Mandatory)][string]$Path, [Parameter(Mandatory)][string]$Name)
    $resolved = Resolve-Path -LiteralPath $Path -ErrorAction SilentlyContinue
    if ($null -eq $resolved -or -not (Test-Path -LiteralPath $resolved.Path -PathType Leaf)) {
        throw "$Name is missing: $Path"
    }
    return $resolved.Path
}

$envPath = Resolve-RequiredFile -Path $EnvFile -Name 'production env file'
$composePath = Resolve-RequiredFile -Path $ComposeFile -Name 'production Compose file'

# Validate only behavior settings and fixed container-path contracts. Values
# are never echoed.
$entries = @{}
foreach ($line in Get-Content -LiteralPath $envPath) {
    if ($line -match '^\s*(?<key>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*(?<value>[^#]*?)\s*$') {
        $value = $Matches.value.Trim()
        if ($value.Length -ge 2 -and
            (($value.StartsWith("'") -and $value.EndsWith("'")) -or
             ($value.StartsWith('"') -and $value.EndsWith('"')))) {
            $value = $value.Substring(1, $value.Length - 2)
        }
        $entries[$Matches.key] = $value
    }
}

$requiredKeys = @('COMPOSE_PROJECT_NAME', 'ALLOW_LOCAL_MEDIA', 'TMDB_MEDIA_BASE_URL')
foreach ($key in $requiredKeys) {
    if (-not $entries.ContainsKey($key) -or [string]::IsNullOrWhiteSpace($entries[$key])) {
        throw "Required deployment setting is missing: $key"
    }
}

$composeText = Get-Content -LiteralPath $composePath -Raw
foreach ($target in @('/media', '/config')) {
    if ($composeText -notmatch "target:\s*$([regex]::Escape($target))") {
        throw "Compose template is missing the fixed container mount: $target"
    }
}
foreach ($service in @('postgres:', 'api:', 'worker:', 'media:')) {
    if ($composeText -notmatch "(?m)^\s{2}$([regex]::Escape($service))") {
        throw "Compose template is missing the canonical service: $service"
    }
}
foreach ($legacy in @('pgbouncer:', 'image-server:', 'admin-migrate:', 'storage-init:')) {
    if ($composeText -match "(?m)^\s{2}$([regex]::Escape($legacy))") {
        throw "Legacy extra service remains in the canonical Compose template: $legacy"
    }
}

$temporaryOutput = [System.IO.Path]::GetTempFileName()
try {
    & docker compose --env-file $envPath --file $composePath config --quiet *> $temporaryOutput
    if ($LASTEXITCODE -ne 0) {
        throw 'Docker Compose rejected the production template or its interpolation.'
    }
}
finally {
    Remove-Item -LiteralPath $temporaryOutput -Force -ErrorAction SilentlyContinue
}

Write-Output 'Production Compose interpolation and fixed four-container contracts passed.'
