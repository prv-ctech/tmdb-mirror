Set-StrictMode -Version Latest

$script:StressSecretNames = @(
    'TMDB_STRESS_READ_TOKEN',
    'TMDB_STRESS_API_KEY',
    'TMDB_STRESS_TRAWL_BASE_URL'
)

function Normalize-StressSecretValue {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Value,
        [Parameter(Mandatory)][string]$ErrorMessage
    )

    $normalized = $Value.Trim()
    if ([string]::IsNullOrWhiteSpace($normalized) -or
        $normalized.IndexOfAny([char[]]@([char]0, [char]9, [char]32)) -ge 0 -or
        $normalized.Contains('"') -or $normalized.Contains("'")) {
        throw $ErrorMessage
    }
    return $normalized
}

function Read-StressSecrets {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$Path)

    $secrets = @{}
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        return $secrets
    }
    if ((Get-Item -LiteralPath $Path).Length -gt 65536) {
        throw 'The local stress secrets file exceeds 64 KiB.'
    }

    $lineNumber = 0
    foreach ($line in [IO.File]::ReadLines($Path, [Text.UTF8Encoding]::new($false))) {
        $lineNumber++
        $trimmed = $line.Trim()
        if ([string]::IsNullOrWhiteSpace($trimmed) -or $trimmed.StartsWith('#', [StringComparison]::Ordinal)) {
            continue
        }
        $match = [regex]::Match(
            $trimmed,
            '^(TMDB_STRESS_READ_TOKEN|TMDB_STRESS_API_KEY|TMDB_STRESS_TRAWL_BASE_URL)=(.+)$'
        )
        if (-not $match.Success) {
            throw "Invalid local stress secret at line $lineNumber."
        }
        $name = $match.Groups[1].Value
        $value = Normalize-StressSecretValue -Value $match.Groups[2].Value `
            -ErrorMessage "Invalid local stress secret at line $lineNumber."
        if ($secrets.ContainsKey($name)) {
            throw "Duplicate local stress secret at line $lineNumber."
        }
        $secrets[$name] = $value
    }
    return $secrets
}

function Resolve-StressSecret {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Secrets,
        [Parameter(Mandatory)][ValidateSet('TMDB_STRESS_READ_TOKEN', 'TMDB_STRESS_API_KEY', 'TMDB_STRESS_TRAWL_BASE_URL')][string]$Name,
        [AllowNull()][string]$ExplicitValue
    )

    if (-not [string]::IsNullOrWhiteSpace($ExplicitValue)) {
        return Normalize-StressSecretValue -Value $ExplicitValue `
            -ErrorMessage "Invalid explicit stress secret for $Name."
    }
    if ($Secrets.ContainsKey($Name)) {
        return [string]$Secrets[$Name]
    }
    return $null
}
