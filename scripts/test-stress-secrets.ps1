[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $PSScriptRoot
. (Join-Path $repoRoot 'scripts/stress-secrets.ps1')

function Assert-Equal {
    param(
        [Parameter(Mandatory)][string]$Name,
        [AllowNull()][string]$Actual,
        [AllowNull()][string]$Expected
    )

    if ($Actual -cne $Expected) {
        throw "$Name did not match the expected value."
    }
}

$temporary = New-TemporaryFile
try {
    [IO.File]::WriteAllLines(
        $temporary,
        @(
            '# local only',
            'TMDB_STRESS_READ_TOKEN=unit-read-token',
            'TMDB_STRESS_API_KEY=unit-v3-api-key',
            'TMDB_STRESS_TRAWL_BASE_URL=http://trawl.example:8191'
        ),
        [Text.UTF8Encoding]::new($false)
    )
    $secrets = Read-StressSecrets -Path $temporary
    Assert-Equal -Name 'read token' -Actual $secrets['TMDB_STRESS_READ_TOKEN'] -Expected 'unit-read-token'
    Assert-Equal -Name 'v3 API key' -Actual $secrets['TMDB_STRESS_API_KEY'] -Expected 'unit-v3-api-key'
    Assert-Equal -Name 'Trawl URL' -Actual $secrets['TMDB_STRESS_TRAWL_BASE_URL'] -Expected 'http://trawl.example:8191'
    Assert-Equal -Name 'explicit value wins' `
        -Actual (Resolve-StressSecret -Secrets $secrets -Name 'TMDB_STRESS_READ_TOKEN' -ExplicitValue 'direct-token') `
        -Expected 'direct-token'

    [IO.File]::WriteAllText(
        $temporary,
        'TMDB_STRESS_READ_TOKEN="quoted-value"',
        [Text.UTF8Encoding]::new($false)
    )
    $rejected = $false
    try {
        $null = Read-StressSecrets -Path $temporary
    }
    catch {
        $rejected = $true
        if ($_.Exception.Message -match 'quoted-value') {
            throw 'Secret parser error exposed a value.'
        }
    }
    if (-not $rejected) {
        throw 'Quoted secret values must be rejected.'
    }

    $explicitRejected = $false
    try {
        $null = Resolve-StressSecret -Secrets $secrets -Name 'TMDB_STRESS_READ_TOKEN' -ExplicitValue '"quoted-value"'
    }
    catch {
        $explicitRejected = $true
        if ($_.Exception.Message -match 'quoted-value') {
            throw 'Explicit secret parser error exposed a value.'
        }
    }
    if (-not $explicitRejected) {
        throw 'Quoted explicit secret values must be rejected.'
    }
}
finally {
    Remove-Item -LiteralPath $temporary -Force -ErrorAction SilentlyContinue
}

Write-Output 'Stress secret loader tests passed.'
