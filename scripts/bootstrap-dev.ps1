[CmdletBinding()]
param(
    [switch]$Rotate
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$projectName = 'tmdb_rust_foundation_test'
$containerName = 'tmdb_rust_foundation_test-postgres-1'
$volumeName = 'tmdb_rust_foundation_test_tmdb_pg18_data'
$networkName = 'tmdb_rust_foundation_test_tmdb-internal'
$loopbackNetworkName = 'tmdb_rust_foundation_test_tmdb-loopback'
$postgresImage = 'postgres:18-bookworm@sha256:1961f96e6029a02c3812d7cb329a3b03a3ac2bb067058dec17b0f5596aca9296'
$repoRoot = Split-Path -Parent $PSScriptRoot
$composePath = Join-Path $repoRoot 'deploy/compose.dev.yaml'
$envPath = Join-Path $repoRoot 'deploy/env.example'
$secretsDirectory = Join-Path $repoRoot 'deploy/secrets'
$secretNames = @(
    'postgres_owner_password',
    'migrator_password',
    'api_reader_password',
    'api_job_submitter_password',
    'ingest_writer_password',
    'image_writer_password',
    'monitor_password'
)

function Invoke-DockerChecked {
    param([Parameter(Mandatory)][string[]]$Arguments)

    $previousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $output = & docker @Arguments 2>&1
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    if ($exitCode -ne 0) {
        $safeOutput = [string]::Join("`n", @($output))
        throw "Docker command failed with exit code $exitCode.`n$safeOutput"
    }
    return [string]::Join("`n", @($output)).Trim()
}

function Get-OptionalDockerInspection {
    param(
        [Parameter(Mandatory)][string[]]$Arguments,
        [Parameter(Mandatory)][string]$MissingPattern
    )

    $previousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $output = & docker @Arguments 2>&1
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    $text = [string]::Join("`n", @($output)).Trim()
    if ($exitCode -eq 0) {
        return $text
    }
    if ($text -match $MissingPattern) {
        return $null
    }
    throw "Docker inspection failed with exit code $exitCode.`n$text"
}

function Assert-PortAvailable {
    $listener = $null
    try {
        $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 55432)
        $listener.Start()
        Write-Output 'Preflight: 127.0.0.1:55432 is available.'
    }
    catch {
        throw 'Preflight refused: 127.0.0.1:55432 is already in use.'
    }
    finally {
        if ($null -ne $listener) {
            $listener.Stop()
        }
    }
}

function Test-ExpectedRuntimePublication {
    $runtimePortsJson = Invoke-DockerChecked -Arguments @('container', 'inspect', '--format', '{{json .NetworkSettings.Ports}}', $containerName)
    $runtimePortMap = $runtimePortsJson | ConvertFrom-Json
    $runtimeBindings = @($runtimePortMap.'5432/tcp')
    return ($runtimeBindings.Count -eq 1 -and
        $runtimeBindings[0].HostIp -ceq '127.0.0.1' -and
        $runtimeBindings[0].HostPort -ceq '55432')
}

function Assert-LoopbackConnectable {
    $tcpClient = [System.Net.Sockets.TcpClient]::new()
    $asyncResult = $null
    try {
        $asyncResult = $tcpClient.BeginConnect('127.0.0.1', 55432, $null, $null)
        if (-not $asyncResult.AsyncWaitHandle.WaitOne(3000)) {
            throw 'Timed out connecting to 127.0.0.1:55432.'
        }
        $tcpClient.EndConnect($asyncResult)
    }
    catch {
        throw 'PostgreSQL is not reachable through 127.0.0.1:55432.'
    }
    finally {
        if ($null -ne $asyncResult) {
            $asyncResult.AsyncWaitHandle.Close()
        }
        $tcpClient.Dispose()
    }
}

function Assert-SecretFile {
    param([Parameter(Mandatory)][string]$Path)

    $bytes = [System.IO.File]::ReadAllBytes($Path)
    if ($bytes.Length -ne 43) {
        throw "Secret file has an invalid byte length: $Path"
    }
    $value = [System.Text.Encoding]::ASCII.GetString($bytes)
    if ($value -cnotmatch '^[A-Za-z0-9_-]{43}$') {
        throw "Secret file is not a 32-byte base64url value: $Path"
    }
}

if (-not (Test-Path -LiteralPath $composePath -PathType Leaf)) {
    throw "Compose definition is missing: $composePath"
}
if (-not (Test-Path -LiteralPath $envPath -PathType Leaf)) {
    throw "Tracked environment example is missing: $envPath"
}

$null = Invoke-DockerChecked -Arguments @('version', '--format', '{{.Server.Version}}')

$containerLabelsJson = Get-OptionalDockerInspection `
    -Arguments @('container', 'inspect', '--format', '{{json .Config.Labels}}', $containerName) `
    -MissingPattern '(?i)no such container'
$volumeLabelsJson = Get-OptionalDockerInspection `
    -Arguments @('volume', 'inspect', '--format', '{{json .Labels}}', $volumeName) `
    -MissingPattern '(?i)no such volume'
$networkLabelsJson = Get-OptionalDockerInspection `
    -Arguments @('network', 'inspect', '--format', '{{json .Labels}}', $networkName) `
    -MissingPattern '(?i)network .* not found'
$loopbackNetworkLabelsJson = Get-OptionalDockerInspection `
    -Arguments @('network', 'inspect', '--format', '{{json .Labels}}', $loopbackNetworkName) `
    -MissingPattern '(?i)network .* not found'

$containerLabels = $null
if ($null -ne $containerLabelsJson) {
    $labels = $containerLabelsJson | ConvertFrom-Json
    $containerLabels = "$($labels.'com.docker.compose.project')|$($labels.'com.docker.compose.service')"
}
$volumeLabels = $null
if ($null -ne $volumeLabelsJson) {
    $labels = $volumeLabelsJson | ConvertFrom-Json
    $volumeLabels = "$($labels.'com.docker.compose.project')|$($labels.'com.docker.compose.volume')"
}
$networkLabels = $null
if ($null -ne $networkLabelsJson) {
    $labels = $networkLabelsJson | ConvertFrom-Json
    $networkLabels = "$($labels.'com.docker.compose.project')|$($labels.'com.docker.compose.network')"
}
$loopbackNetworkLabels = $null
if ($null -ne $loopbackNetworkLabelsJson) {
    $labels = $loopbackNetworkLabelsJson | ConvertFrom-Json
    $loopbackNetworkLabels = "$($labels.'com.docker.compose.project')|$($labels.'com.docker.compose.network')"
}

if ($null -ne $containerLabels -and $containerLabels -cne "$projectName|postgres") {
    throw "Preflight refused unexpected container ownership for $containerName."
}
if ($null -ne $volumeLabels -and $volumeLabels -cne "$projectName|tmdb_pg18_data") {
    throw "Preflight refused unexpected volume ownership for $volumeName."
}
if ($null -ne $networkLabels -and $networkLabels -cne "$projectName|tmdb-internal") {
    throw "Preflight refused unexpected network ownership for $networkName."
}
if ($null -ne $loopbackNetworkLabels -and $loopbackNetworkLabels -cne "$projectName|tmdb-loopback") {
    throw "Preflight refused unexpected network ownership for $loopbackNetworkName."
}

if ($null -ne $networkLabels) {
    $networkConfiguration = Invoke-DockerChecked -Arguments @('network', 'inspect', '--format', '{{.Internal}}|{{.Driver}}', $networkName)
    if ($networkConfiguration -cne 'true|bridge') {
        throw "Preflight refused unexpected network configuration for $networkName."
    }
}
if ($null -ne $loopbackNetworkLabels) {
    $networkConfiguration = Invoke-DockerChecked -Arguments @('network', 'inspect', '--format', '{{.Internal}}|{{.Driver}}', $loopbackNetworkName)
    if ($networkConfiguration -cne 'false|bridge') {
        throw "Preflight refused unexpected network configuration for $loopbackNetworkName."
    }
}

$resourceExists = ($null -ne $containerLabels) -or ($null -ne $volumeLabels) -or
    ($null -ne $networkLabels) -or ($null -ne $loopbackNetworkLabels)
if ($Rotate -and $resourceExists) {
    throw 'Secret rotation refused because the exact test cluster already has Docker resources.'
}

$missingSecretNames = @($secretNames | Where-Object {
    -not (Test-Path -LiteralPath (Join-Path $secretsDirectory $_) -PathType Leaf)
})
if ($resourceExists -and $missingSecretNames.Count -gt 0) {
    throw "Secret generation refused because exact test cluster resources exist and required development secret files are missing: $($missingSecretNames -join ', ')."
}

if ($null -ne $containerLabels) {
    $configuredImage = Invoke-DockerChecked -Arguments @('container', 'inspect', '--format', '{{.Config.Image}}', $containerName)
    if ($configuredImage -cne $postgresImage) {
        throw "Preflight refused unexpected image for $containerName."
    }
    $bindingsJson = Invoke-DockerChecked -Arguments @('container', 'inspect', '--format', '{{json .HostConfig.PortBindings}}', $containerName)
    $bindingMap = $bindingsJson | ConvertFrom-Json
    $bindings = @($bindingMap.'5432/tcp')
    if ($bindings.Count -ne 1 -or $bindings[0].HostIp -cne '127.0.0.1' -or $bindings[0].HostPort -cne '55432') {
        throw "Preflight refused unexpected published-port configuration for $containerName."
    }
    $running = Invoke-DockerChecked -Arguments @('container', 'inspect', '--format', '{{.State.Running}}', $containerName)
    if ($running -cne 'true' -or -not (Test-ExpectedRuntimePublication)) {
        Assert-PortAvailable
    }
    else {
        Write-Output 'Preflight: the verified test container owns 127.0.0.1:55432.'
    }
}
else {
    Assert-PortAvailable
}

if (-not (Test-Path -LiteralPath $secretsDirectory -PathType Container)) {
    New-Item -ItemType Directory -Path $secretsDirectory | Out-Null
}

foreach ($secretName in $secretNames) {
    $secretPath = Join-Path $secretsDirectory $secretName
    if ((Test-Path -LiteralPath $secretPath -PathType Leaf) -and -not $Rotate) {
        Assert-SecretFile -Path $secretPath
        Write-Output "Preserved existing development secret: $secretName"
        continue
    }

    $staticGetBytes = [System.Security.Cryptography.RandomNumberGenerator].GetMethod(
        'GetBytes', [Type[]]@([int])
    )
    if ($null -ne $staticGetBytes) {
        $bytes = [System.Security.Cryptography.RandomNumberGenerator]::GetBytes(32)
    }
    else {
        # Windows PowerShell 5.1 targets .NET Framework, which lacks the static overload.
        $bytes = New-Object byte[] 32
        $generator = [System.Security.Cryptography.RandomNumberGenerator]::Create()
        try {
            $generator.GetBytes($bytes)
        }
        finally {
            $generator.Dispose()
        }
    }
    $value = [Convert]::ToBase64String($bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_')
    [System.IO.File]::WriteAllText($secretPath, $value, [System.Text.UTF8Encoding]::new($false))
    Assert-SecretFile -Path $secretPath
    Write-Output "Generated development secret: $secretName"
}

$composeArguments = @(
    'compose', '--env-file', $envPath, '-p', $projectName,
    '-f', $composePath, 'up', '-d', '--wait', 'postgres'
)
$composeOutput = Invoke-DockerChecked -Arguments $composeArguments
if ($composeOutput) {
    Write-Output $composeOutput
}
if (-not (Test-ExpectedRuntimePublication)) {
    throw 'Compose finished without the exact runtime publication 127.0.0.1:55432->5432/tcp.'
}
Assert-LoopbackConnectable
Write-Output 'Runtime publication is reachable at 127.0.0.1:55432.'
Write-Output "PostgreSQL development cluster is healthy under project $projectName."
