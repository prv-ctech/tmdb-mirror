[CmdletBinding()]
param(
    [ValidateSet('endpoints', 'burn', 'full')]
    [string]$Profile = 'full',

    [string]$BaseUrl = 'http://127.0.0.1:18080',

    [ValidateRange(1, 1000)]
    [int]$VirtualUsers = 100,

    [ValidateRange(1, 2000000)]
    [int]$RequestsPerEndpoint = 10000,

    [ValidateRange(1, 5000000)]
    [int]$BurnRequests = 100000,

    [ValidatePattern('^[1-9][0-9]*[smh]$')]
    [string]$MaxDuration = '30m',

    [ValidateRange(1, 300)]
    [int]$RequestTimeoutSeconds = 30,

    # Optional representative, already-ingested API paths. These are relative
    # paths only; they never accept an origin, credential, or header value.
    [string]$MetadataPath = '',

    [string]$ListPath = '',

    [string]$SearchPath = '',

    [string]$FilterPath = '',

    [string]$K6Image = 'grafana/k6:1.0.0@sha256:f21270290d702cbf0a7d6ba5d7ed100b63bcb233b558b885ed787547b3488279',

    [string]$ResultsDirectory = '',

    [string]$Network = '',

    [string]$ComposeFile = '',

    [string]$ComposeEnvFile = '',

    [string]$ComposeProjectName = '',

    [string]$AdminMetricsUrl = '',

    [ValidatePattern('^[A-Za-z_][A-Za-z0-9_]{0,127}$')]
    [string]$AdminKeyEnvironmentVariable = 'TMDB_K6_ADMIN_API_KEY'
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$scenarioPath = Join-Path $PSScriptRoot 'tmdb-api.js'
$collectorPath = Join-Path $PSScriptRoot 'collect-diagnostics.ps1'

function ConvertTo-HttpOrigin {
    param(
        [Parameter(Mandatory)][string]$Value,
        [Parameter(Mandatory)][string]$Name
    )

    $uri = $null
    if (-not [Uri]::TryCreate($Value.Trim(), [UriKind]::Absolute, [ref]$uri) -or
        $uri.Scheme -notin @('http', 'https') -or
        [string]::IsNullOrWhiteSpace($uri.Host) -or
        -not [string]::IsNullOrWhiteSpace($uri.UserInfo) -or
        -not [string]::IsNullOrWhiteSpace($uri.Query) -or
        -not [string]::IsNullOrWhiteSpace($uri.Fragment) -or
        ($uri.AbsolutePath -ne '/' -and $uri.AbsolutePath -ne '')) {
        throw "$Name must be an http(s) origin without credentials, paths, a query string, or a fragment."
    }

    return $uri.GetLeftPart([UriPartial]::Authority).TrimEnd('/')
}

function ConvertTo-K6ReachableOrigin {
    param([Parameter(Mandatory)][string]$Origin)

    $uri = [Uri]$Origin
    if (-not $uri.IsLoopback) {
        return $Origin
    }

    $builder = [UriBuilder]::new($uri)
    $builder.Host = 'host.docker.internal'
    return $builder.Uri.GetLeftPart([UriPartial]::Authority).TrimEnd('/')
}

function ConvertTo-SafeHttpUrl {
    param(
        [Parameter(Mandatory)][string]$Value,
        [Parameter(Mandatory)][string]$Name
    )

    $uri = $null
    if (-not [Uri]::TryCreate($Value.Trim(), [UriKind]::Absolute, [ref]$uri) -or
        $uri.Scheme -notin @('http', 'https') -or
        [string]::IsNullOrWhiteSpace($uri.Host) -or
        -not [string]::IsNullOrWhiteSpace($uri.UserInfo) -or
        -not [string]::IsNullOrWhiteSpace($uri.Query) -or
        -not [string]::IsNullOrWhiteSpace($uri.Fragment)) {
        throw "$Name must be an http(s) URL without credentials, a query string, or a fragment."
    }

    return $uri.AbsoluteUri
}

function ConvertTo-RelativeApiPath {
    param(
        [AllowEmptyString()][string]$Value,
        [Parameter(Mandatory)][string]$Name
    )

    if ([string]::IsNullOrWhiteSpace($Value)) {
        return ''
    }
    $path = $Value.Trim()
    if ($path.Length -gt 2048 -or -not $path.StartsWith('/') -or $path.StartsWith('//') -or
        $path.Contains("`r") -or $path.Contains("`n")) {
        throw "$Name must be a relative API path no longer than 2048 characters."
    }
    return $path
}

function Protect-Text {
    param([AllowNull()][string]$Text)

    if ($null -eq $Text) {
        return ''
    }

    $protected = $Text
    $protected = [regex]::Replace(
        $protected,
        '(?im)\b(authorization|x-api-key)\s*[:=]\s*(?:bearer\s+)?[^\s,;]+',
        '$1=<redacted>'
    )
    $protected = [regex]::Replace(
        $protected,
        '(?im)\b(password|token|api[_-]?key|secret)\s*[:=]\s*[^\s,;]+',
        '$1=<redacted>'
    )
    $protected = [regex]::Replace(
        $protected,
        '([A-Za-z][A-Za-z0-9+.-]*://[^\s:/]+:)[^@\s/]+@',
        '$1<redacted>@'
    )
    $protected = [regex]::Replace(
        $protected,
        '\beyJ[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+){2}\b',
        '<redacted-token>'
    )
    return $protected
}

function Write-Utf8NoBom {
    param(
        [Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][string]$Content
    )

    [IO.File]::WriteAllText($Path, $Content, [Text.UTF8Encoding]::new($false))
}

function Assert-DockerAvailable {
    $version = Invoke-DockerCapture -Arguments @('version', '--format', '{{.Server.Version}}')
    if ($version.ExitCode -ne 0) {
        throw "Docker is unavailable. $($version.Output)"
    }
}

function Invoke-DockerCapture {
    param([Parameter(Mandatory)][string[]]$Arguments)

    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $rawOutput = @(& docker @Arguments 2>&1)
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }

    return [pscustomobject]@{
        ExitCode = $exitCode
        Output = [string]::Join("`n", @($rawOutput))
    }
}

function Invoke-K6Run {
    param(
        [Parameter(Mandatory)][ValidateSet('endpoint', 'burn')][string]$Mode,
        [AllowEmptyString()][string]$EndpointClass,
        [Parameter(Mandatory)][int]$Iterations,
        [Parameter(Mandatory)][string]$RunName,
        [Parameter(Mandatory)][string]$ContainerOrigin,
        [Parameter(Mandatory)][string]$OutputDirectory
    )

    $stamp = [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ')
    $summaryName = "k6-$RunName-$stamp.summary.json"
    $consoleName = "k6-$RunName-$stamp.console.txt"
    $summaryPath = Join-Path $OutputDirectory $summaryName
    $consolePath = Join-Path $OutputDirectory $consoleName

    $arguments = [Collections.Generic.List[string]]::new()
    [void]$arguments.Add('run')
    [void]$arguments.Add('--rm')
    [void]$arguments.Add('--init')
    [void]$arguments.Add('--add-host')
    [void]$arguments.Add('host.docker.internal:host-gateway')

    if (-not [string]::IsNullOrWhiteSpace($Network)) {
        [void]$arguments.Add('--network')
        [void]$arguments.Add($Network)
    }

    $isWindows = [Environment]::OSVersion.Platform -eq [PlatformID]::Win32NT
    if (-not $isWindows) {
        $uid = ([string](& id -u)).Trim()
        $gid = ([string](& id -g)).Trim()
        if ($uid -match '^[0-9]+$' -and $gid -match '^[0-9]+$') {
            [void]$arguments.Add('--user')
            [void]$arguments.Add("$uid`:$gid")
        }
    }

    [void]$arguments.Add('-v')
    [void]$arguments.Add("$scenarioPath`:/scripts/tmdb-api.js:ro")
    [void]$arguments.Add('-v')
    [void]$arguments.Add("$OutputDirectory`:/results")
    [void]$arguments.Add($K6Image)
    [void]$arguments.Add('run')

    $k6Environment = @(
        "TMDB_K6_BASE_URL=$ContainerOrigin",
        "TMDB_K6_RUN_MODE=$Mode",
        "TMDB_K6_VUS=$VirtualUsers",
        "TMDB_K6_ITERATIONS=$Iterations",
        "TMDB_K6_MAX_DURATION=$MaxDuration",
        "TMDB_K6_REQUEST_TIMEOUT=$($RequestTimeoutSeconds)s"
    )
    foreach ($setting in $endpointPathOverrides.GetEnumerator()) {
        if (-not [string]::IsNullOrWhiteSpace([string]$setting.Value)) {
            $k6Environment += "$($setting.Key)=$($setting.Value)"
        }
    }
    if ($Mode -eq 'endpoint') {
        $k6Environment += "TMDB_K6_ENDPOINT_CLASS=$EndpointClass"
    }
    foreach ($entry in $k6Environment) {
        [void]$arguments.Add('--env')
        [void]$arguments.Add($entry)
    }

    [void]$arguments.Add("--summary-export=/results/$summaryName")
    [void]$arguments.Add('/scripts/tmdb-api.js')

    $dockerResult = Invoke-DockerCapture -Arguments $arguments.ToArray()
    $exitCode = $dockerResult.ExitCode
    $output = Protect-Text -Text $dockerResult.Output
    Write-Utf8NoBom -Path $consolePath -Content ($output + "`n")

    if (Test-Path -LiteralPath $summaryPath -PathType Leaf) {
        $summary = Protect-Text -Text ([IO.File]::ReadAllText($summaryPath, [Text.UTF8Encoding]::new($false)))
        Write-Utf8NoBom -Path $summaryPath -Content ($summary + "`n")
    }

    Write-Verbose "k6 $RunName exit code: $exitCode"
    Write-Verbose "k6 console artifact: $consolePath"
    if (Test-Path -LiteralPath $summaryPath -PathType Leaf) {
        Write-Verbose "k6 summary artifact: $summaryPath"
    }

    return [pscustomobject]@{
        ExitCode = $exitCode
        RunName = $RunName
        SummaryPath = $summaryPath
        ConsolePath = $consolePath
    }
}

if (-not (Test-Path -LiteralPath $scenarioPath -PathType Leaf)) {
    throw "k6 scenario is missing: $scenarioPath"
}
if (-not (Test-Path -LiteralPath $collectorPath -PathType Leaf)) {
    throw "Failure diagnostics collector is missing: $collectorPath"
}
if ($K6Image -notmatch '^[a-z0-9][a-z0-9._/-]*(?::[A-Za-z0-9._-]+)?@sha256:[a-f0-9]{64}$') {
    throw 'K6Image must be an immutable image reference pinned by a sha256 digest.'
}
if (-not [string]::IsNullOrWhiteSpace($Network) -and $Network -notmatch '^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$') {
    throw 'Network must be a Docker network name.'
}
foreach ($pathSetting in @($ComposeFile, $ComposeEnvFile)) {
    if (-not [string]::IsNullOrWhiteSpace($pathSetting) -and -not (Test-Path -LiteralPath $pathSetting -PathType Leaf)) {
        throw "Configured Compose file does not exist: $pathSetting"
    }
}

$hostOrigin = ConvertTo-HttpOrigin -Value $BaseUrl -Name 'BaseUrl'
$containerOrigin = ConvertTo-K6ReachableOrigin -Origin $hostOrigin
$endpointPathOverrides = [ordered]@{
    TMDB_K6_METADATA_PATH = ConvertTo-RelativeApiPath -Value $MetadataPath -Name 'MetadataPath'
    TMDB_K6_LIST_PATH = ConvertTo-RelativeApiPath -Value $ListPath -Name 'ListPath'
    TMDB_K6_SEARCH_PATH = ConvertTo-RelativeApiPath -Value $SearchPath -Name 'SearchPath'
    TMDB_K6_FILTER_PATH = ConvertTo-RelativeApiPath -Value $FilterPath -Name 'FilterPath'
}
if (-not [string]::IsNullOrWhiteSpace($AdminMetricsUrl)) {
    $null = ConvertTo-SafeHttpUrl -Value $AdminMetricsUrl -Name 'AdminMetricsUrl'
}

Assert-DockerAvailable

$timestamp = [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ')
if ([string]::IsNullOrWhiteSpace($ResultsDirectory)) {
    $ResultsDirectory = Join-Path $repoRoot ".stress-runtime\k6\$timestamp"
}
New-Item -ItemType Directory -Path $ResultsDirectory -Force | Out-Null
$ResultsDirectory = (Resolve-Path -LiteralPath $ResultsDirectory).Path

$manifest = [ordered]@{
    schema_version = 1
    started_at_utc = [DateTime]::UtcNow.ToString('O')
    profile = $Profile
    virtual_users = $VirtualUsers
    requests_per_endpoint = $RequestsPerEndpoint
    burn_requests = $BurnRequests
    max_duration = $MaxDuration
    request_timeout_seconds = $RequestTimeoutSeconds
    k6_image = $K6Image
    uses_custom_network = -not [string]::IsNullOrWhiteSpace($Network)
    has_compose_diagnostics = -not [string]::IsNullOrWhiteSpace($ComposeProjectName)
    has_admin_pool_metrics = -not [string]::IsNullOrWhiteSpace($AdminMetricsUrl)
}
Write-Utf8NoBom -Path (Join-Path $ResultsDirectory 'run.json') -Content (($manifest | ConvertTo-Json -Depth 4) + "`n")

$runs = [Collections.Generic.List[object]]::new()
if ($Profile -in @('endpoints', 'full')) {
    foreach ($endpoint in @('metadata', 'list', 'search', 'filter')) {
        $runs.Add((Invoke-K6Run -Mode 'endpoint' -EndpointClass $endpoint -Iterations $RequestsPerEndpoint `
            -RunName "endpoint-$endpoint" -ContainerOrigin $containerOrigin -OutputDirectory $ResultsDirectory))
        if ($runs[$runs.Count - 1].ExitCode -ne 0) { break }
    }
}
if (($Profile -in @('burn', 'full')) -and ($runs.Count -eq 0 -or $runs[$runs.Count - 1].ExitCode -eq 0)) {
    $runs.Add((Invoke-K6Run -Mode 'burn' -EndpointClass '' -Iterations $BurnRequests `
        -RunName 'burn' -ContainerOrigin $containerOrigin -OutputDirectory $ResultsDirectory))
}

$failure = @($runs | Where-Object { $_.ExitCode -ne 0 } | Select-Object -First 1)
if ($failure.Count -gt 0) {
    Write-Warning "k6 failed during $($failure[0].RunName); collecting redacted diagnostics."
    try {
        & $collectorPath -ResultDirectory $ResultsDirectory -ComposeFile $ComposeFile -ComposeEnvFile $ComposeEnvFile `
            -ComposeProjectName $ComposeProjectName -AdminMetricsUrl $AdminMetricsUrl `
            -AdminKeyEnvironmentVariable $AdminKeyEnvironmentVariable `
            -RunStartedAtUtc $manifest.started_at_utc
    }
    catch {
        Write-Warning "Diagnostic collection failed: $(Protect-Text -Text $_.Exception.Message)"
    }
    exit ([int]$failure[0].ExitCode)
}

Write-Output "k6 $Profile profile passed. Artifacts: $ResultsDirectory"
