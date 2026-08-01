[CmdletBinding(SupportsShouldProcess)]
param(
    [string]$ProjectName = 'tmdb_stress_test',
    [switch]$RemoveVolumes
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $repoRoot 'deploy/compose.stress.yaml'
$runtimeRoot = Join-Path (Join-Path $repoRoot '.stress-runtime') $ProjectName
$envFile = Join-Path $runtimeRoot 'compose.env'

if (-not (Test-Path -LiteralPath $envFile -PathType Leaf)) {
    throw "Runtime environment is missing: $envFile"
}

$composeArgs = @('compose', '--env-file', $envFile, '--project-name', $ProjectName, '--file', $composeFile, 'down', '--remove-orphans')
if ($RemoveVolumes) { $composeArgs += '--volumes' }
if ($PSCmdlet.ShouldProcess($ProjectName, "Stop stress project$(if ($RemoveVolumes) { ' and remove its named volumes' })")) {
    & docker @composeArgs
    if ($LASTEXITCODE -ne 0) { throw "Stress project teardown failed with exit code $LASTEXITCODE." }
}
