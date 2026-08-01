[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $PSScriptRoot
$scriptRelativePath = 'scripts/verify-repository-hygiene.ps1'

$trackedFiles = @(& git -C $repoRoot ls-files)
if ($LASTEXITCODE -ne 0) {
    throw 'Could not enumerate tracked files.'
}

# These rules deliberately report only a rule name and source location. They
# never print the matching line, which prevents this check from leaking a
# credential while reporting a failure.
$rules = [ordered]@{
    'TMDB/JWT credential' = '(?<![A-Za-z0-9_-])eyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}(?![A-Za-z0-9_-])'
    'URI credentials' = '(?i)\bhttps?://[^/\s:@]+:[^@\s/]{16,}@'
    'URL credential query' = '(?i)[?&](?:token|api[_-]?key|access[_-]?token|password)=[A-Za-z0-9._~+/=-]{16,}'
    'private IPv4 address' = '\b(?:10\.(?:\d{1,3}\.){2}\d{1,3}|192\.168\.(?:\d{1,3}\.)\d{1,3}|172\.(?:1[6-9]|2\d|3[01])\.(?:\d{1,3}\.)\d{1,3})\b'
    'host-specific filesystem path' = '(?i)(?:[A-Z]:\\Users\\|/Users/[^/\s]+/|/mnt/(?:user|cache|disk|pool)/)'
    'hardcoded API credential assignment' = '(?i)\b(?:TMDB_API_KEY|TMDB_READ_ACCESS_TOKEN|API_KEY|ACCESS_TOKEN)\s*[:=]\s*["'']?(?!<|your[-_]|example|placeholder|changeme|test[-_]|unit[-_])[A-Za-z0-9._~+/=-]{16,}'
}

$violations = [System.Collections.Generic.List[string]]::new()
foreach ($relativePath in $trackedFiles) {
    $normalizedPath = $relativePath.Replace('\', '/')
    if ($normalizedPath -eq $scriptRelativePath) { continue }
    $fullPath = Join-Path $repoRoot $relativePath
    if (-not (Test-Path -LiteralPath $fullPath -PathType Leaf)) { continue }

    $lineNumber = 0
    foreach ($line in [System.IO.File]::ReadLines($fullPath)) {
        $lineNumber++
        # This exact marker is a negative test fixture, not a credential.
        if ($line -match '(?i)must-not-appear') { continue }
        foreach ($rule in $rules.GetEnumerator()) {
            if ($line -match $rule.Value) {
                $violations.Add("$($rule.Key) at $normalizedPath`:$lineNumber")
            }
        }
    }
}

if ($violations.Count -gt 0) {
    throw "Repository hygiene failed:`n$([string]::Join("`n", $violations))"
}

$ignoredProbes = @(
    '.env',
    '.stress-runtime/example/token',
    'deploy/secrets/example-secret',
    'target/debug/example',
    'example.log'
)
foreach ($probe in $ignoredProbes) {
    & git -C $repoRoot check-ignore --no-index --quiet -- $probe
    if ($LASTEXITCODE -ne 0) {
        throw "Git ignore policy does not cover generated path: $probe"
    }
}

& git -C $repoRoot check-ignore --no-index --quiet -- deploy/secrets/README.md
if ($LASTEXITCODE -eq 0) {
    throw 'The tracked deploy/secrets/README.md must remain visible to Git.'
}

Write-Output "Repository hygiene passed for $($trackedFiles.Count) tracked files."
