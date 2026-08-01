[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$projectName = 'tmdb_rust_foundation_test'
$containerName = "$projectName-postgres-1"
$repoRoot = Split-Path -Parent $PSScriptRoot
$composePath = Join-Path $repoRoot 'deploy/compose.dev.yaml'
$envPath = Join-Path $repoRoot 'deploy/env.example'

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
        throw "Docker command failed with exit code $exitCode.`n$([string]::Join("`n", $output))"
    }
    return [string]::Join("`n", $output).Trim()
}

function Assert-PortAvailable {
    $listener = $null
    try {
        $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 55432)
        $listener.Start()
    }
    catch {
        throw '127.0.0.1:55432 is already in use.'
    }
    finally {
        if ($null -ne $listener) { $listener.Stop() }
    }
}

function Assert-LoopbackConnectable {
    $client = [System.Net.Sockets.TcpClient]::new()
    $async = $null
    try {
        $async = $client.BeginConnect('127.0.0.1', 55432, $null, $null)
        if (-not $async.AsyncWaitHandle.WaitOne(3000)) { throw 'connection timeout' }
        $client.EndConnect($async)
    }
    catch {
        throw 'PostgreSQL is not reachable at 127.0.0.1:55432.'
    }
    finally {
        if ($null -ne $async) { $async.AsyncWaitHandle.Close() }
        $client.Dispose()
    }
}

if (-not (Test-Path -LiteralPath $composePath -PathType Leaf)) {
    throw "Compose definition is missing: $composePath"
}
if (-not (Test-Path -LiteralPath $envPath -PathType Leaf)) {
    throw "Development environment file is missing: $envPath"
}

$null = Invoke-DockerChecked -Arguments @('version', '--format', '{{.Server.Version}}')
try {
    Assert-PortAvailable
}
catch {
    try {
        $existing = Invoke-DockerChecked -Arguments @('container', 'inspect', '--format', '{{.Config.Labels}}|{{.State.Running}}', $containerName)
        if ($existing -notmatch "$projectName.*postgres.*true") { throw }
    }
    catch {
        throw '127.0.0.1:55432 is occupied by an unrelated process.'
    }
}

$composeArgs = @('compose', '--env-file', $envPath, '--project-name', $projectName, '--file', $composePath)
$null = Invoke-DockerChecked -Arguments ($composeArgs + @('config', '--quiet'))
Invoke-DockerChecked -Arguments ($composeArgs + @('up', '-d', '--wait', 'postgres')) | Write-Output

$published = Invoke-DockerChecked -Arguments @('container', 'inspect', '--format', '{{json .NetworkSettings.Ports}}', $containerName)
$ports = $published | ConvertFrom-Json
$binding = @($ports.'5432/tcp')
if ($binding.Count -ne 1 -or $binding[0].HostIp -cne '127.0.0.1' -or $binding[0].HostPort -cne '55432') {
    throw 'The development PostgreSQL port is not exactly 127.0.0.1:55432->5432.'
}
Assert-LoopbackConnectable
Write-Output "PostgreSQL development cluster is healthy under project $projectName."
