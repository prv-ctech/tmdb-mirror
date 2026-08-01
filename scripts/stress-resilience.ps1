[CmdletBinding()]
param(
    [string]$ProjectName = 'tmdb_stress_test',
    [int]$ApiPort = 18080,
    [int]$TimeoutSeconds = 90
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $repoRoot 'deploy/compose.stress.yaml'
$runtimeRoot = Join-Path (Join-Path $repoRoot '.stress-runtime') $ProjectName
$envFile = Join-Path $runtimeRoot 'compose.env'
$resultRoot = Join-Path $runtimeRoot 'results'
$stamp = [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ')

if (-not (Test-Path -LiteralPath $envFile -PathType Leaf)) {
    throw "Runtime environment is missing: $envFile. Run stress-bootstrap.ps1 first."
}
New-Item -ItemType Directory -Force -Path $resultRoot | Out-Null
$composeArgs = @('compose', '--env-file', $envFile, '--project-name', $ProjectName, '--file', $composeFile)

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

function Get-HttpStatus {
    param([Parameter(Mandatory)][string]$Uri)

    $output = @(& curl.exe --silent --show-error --max-time 5 --output NUL --write-out '%{http_code}' $Uri 2>&1)
    if ($LASTEXITCODE -ne 0) { return 0 }
    $value = ([string]::Join('', $output)).Trim()
    if ($value -notmatch '^\d{3}$') { return 0 }
    return [int]$value
}

function Wait-Healthy {
    param(
        [Parameter(Mandatory)][string]$Service,
        [int]$WaitSeconds = 90
    )

    $deadline = [DateTime]::UtcNow.AddSeconds($WaitSeconds)
    do {
        $containerId = (Invoke-DockerChecked -Arguments ($composeArgs + @('ps', '-q', $Service))).Trim()
        if ($containerId) {
            $state = Invoke-DockerChecked -Arguments @('inspect', '--format', '{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{end}}', $containerId)
            if ($state -eq 'running|healthy') { return }
        }
        Start-Sleep -Seconds 2
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Timed out waiting for '$Service' to become healthy."
}

function Wait-Running {
    param(
        [Parameter(Mandatory)][string]$Service,
        [int]$WaitSeconds = 30
    )

    $deadline = [DateTime]::UtcNow.AddSeconds($WaitSeconds)
    do {
        $containerId = (Invoke-DockerChecked -Arguments ($composeArgs + @('ps', '-q', $Service))).Trim()
        if ($containerId) {
            $state = Invoke-DockerChecked -Arguments @('inspect', '--format', '{{.State.Status}}', $containerId)
            if ($state -eq 'running') { return $state }
        }
        Start-Sleep -Seconds 1
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Timed out waiting for '$Service' to return to running."
}

$apiReadyUri = "http://127.0.0.1:$ApiPort/health/ready"
$startedAt = [DateTime]::UtcNow
$before = Get-HttpStatus -Uri $apiReadyUri
if ($before -ne 200) {
    throw "API is not ready before resilience checks (HTTP $before)."
}

$imageId = (Invoke-DockerChecked -Arguments ($composeArgs + @('ps', '-q', 'media'))).Trim()
$imageBefore = Invoke-DockerChecked -Arguments @('inspect', '--format', '{{.State.Status}}', $imageId)
Invoke-DockerChecked -Arguments ($composeArgs + @('restart', '-t', '10', 'media')) | Out-Null
$imageAfter = Wait-Running -Service 'media'

$during = 0
$after = 0
$dependencyStopped = $false
try {
    Invoke-DockerChecked -Arguments ($composeArgs + @('stop', 'postgres')) | Out-Null
    $dependencyStopped = $true
    Start-Sleep -Seconds 3
    $during = Get-HttpStatus -Uri $apiReadyUri
}
finally {
    if ($dependencyStopped) {
        Invoke-DockerChecked -Arguments ($composeArgs + @('start', 'postgres')) | Out-Null
        Wait-Healthy -Service 'postgres' -WaitSeconds $TimeoutSeconds
    }
}

$recoveryDeadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
do {
    $after = Get-HttpStatus -Uri $apiReadyUri
    if ($after -eq 200) { break }
    Start-Sleep -Seconds 2
} while ([DateTime]::UtcNow -lt $recoveryDeadline)

# A database restart terminates idle SQL connections. The worker processes use
# the same bounded pools as the API, so the deployment restart policy must
# bring them back after PostgreSQL is healthy instead of leaving a partial
# stack running.
$workerAfter = Wait-Running -Service 'worker' -WaitSeconds $TimeoutSeconds
$mediaAfter = Wait-Running -Service 'media' -WaitSeconds $TimeoutSeconds

$logs = Invoke-DockerChecked -Arguments ($composeArgs + @('logs', '--no-color', '--timestamps', 'api', 'postgres', 'worker', 'media'))
$logPath = Join-Path $resultRoot "resilience-logs-$stamp.txt"
[IO.File]::WriteAllText($logPath, $logs + "`n", [Text.UTF8Encoding]::new($false))

$result = [ordered]@{
    checked_at_utc = [DateTime]::UtcNow.ToString('O')
    project = $ProjectName
    api_ready_url = $apiReadyUri
    worker_restart = [ordered]@{
        service = 'media'
        before = $imageBefore
        after = $imageAfter
        passed = ($imageBefore -eq 'running' -and $imageAfter -eq 'running')
    }
    dependency_recovery = [ordered]@{
        service = 'postgres'
        api_before_http = $before
        api_during_http = $during
        api_after_http = $after
        dependency_failure_observed = ($during -ne 200)
        recovered = ($after -eq 200)
        worker_after = $workerAfter
        media_after = $mediaAfter
        workers_recovered = ($workerAfter -eq 'running' -and $mediaAfter -eq 'running')
    }
    log_artifact = $logPath
    elapsed_seconds = ([DateTime]::UtcNow - $startedAt).TotalSeconds
}
$result.passed = [bool]($result.worker_restart.passed -and $result.dependency_recovery.dependency_failure_observed -and $result.dependency_recovery.recovered -and $result.dependency_recovery.workers_recovered)
$resultPath = Join-Path $resultRoot "resilience-$stamp.json"
[IO.File]::WriteAllText($resultPath, (($result | ConvertTo-Json -Depth 8) + "`n"), [Text.UTF8Encoding]::new($false))

if (-not $result.passed) {
    throw "Resilience checks failed. See $resultPath and $logPath"
}
Write-Output "Resilience checks passed: $resultPath"
