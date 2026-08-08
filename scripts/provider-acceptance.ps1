[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$Client,
    [Parameter(Mandatory = $true)][string[]]$ClientCommand,
    [string]$StateDir,
    [string]$ComposeFile,
    [string]$ProjectPath = (Get-Location).Path,
    [string]$Python = "python",
    [int]$TimeoutSeconds = 180,
    [switch]$ApplyConfiguration,
    [string]$ExpectedSourceName,
    [switch]$KeepState
)

$ErrorActionPreference = "Stop"

$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
if ([string]::IsNullOrWhiteSpace($ComposeFile)) {
    $ComposeFile = Join-Path $repositoryRoot "compose.yaml"
}
$composePath = (Resolve-Path $ComposeFile).Path
$projectPath = (Resolve-Path $ProjectPath).Path
$composeProject = "llm-observatory-provider-acceptance-$([Guid]::NewGuid().ToString('N').Substring(0, 12))"
$previousComposeProject = $env:COMPOSE_PROJECT_NAME
$env:COMPOSE_PROJECT_NAME = $composeProject
$temporaryState = [string]::IsNullOrWhiteSpace($StateDir)
if ($temporaryState) {
    $StateDir = Join-Path ([IO.Path]::GetTempPath()) "llm-observatory-provider-acceptance-$([Guid]::NewGuid().ToString('N'))"
}
$statePath = [IO.Path]::GetFullPath($StateDir)
$previousPythonPath = $env:PYTHONPATH
$previousOtlpGrpcEndpoint = $env:OBSERVATORY_OTLP_GRPC_ENDPOINT
$previousOtlpHttpEndpoint = $env:OBSERVATORY_OTLP_HTTP_ENDPOINT
$previousOtelResourceAttributes = $env:OTEL_RESOURCE_ATTRIBUTES
$previousOtelExporterEndpoint = $env:OTEL_EXPORTER_OTLP_ENDPOINT
$previousOtelExporterProtocol = $env:OTEL_EXPORTER_OTLP_PROTOCOL
$env:PYTHONPATH = Join-Path $repositoryRoot "src"
$gitRoot = $null
$canonicalClient = $null
$runtimeComposeFile = $null
$acceptanceRunId = "provider-acceptance-$([Guid]::NewGuid().ToString('N'))"
$acceptanceResourceAttribute = "llm.observatory.acceptance.run_id=$acceptanceRunId"
$script:ApiBase = "http://127.0.0.1:8787"
$script:CollectorBase = "http://127.0.0.1:13133"

function Assert-True {
    param([Parameter(Mandatory = $true)][bool]$Condition, [Parameter(Mandatory = $true)][string]$Message)
    if (-not $Condition) {
        throw $Message
    }
}

function Invoke-ObservatoryCli {
    param(
        [Parameter(Mandatory = $true)][string[]]$CliArgs,
        [int[]]$AllowedExitCodes = @(0)
    )

    $stderrPath = Join-Path $statePath ".cli-stderr"
    $arguments = @("-m", "observatory.cli", "--json", "--timeout", [string]$TimeoutSeconds, "--state-dir", $statePath) + $CliArgs
    $output = & $Python @arguments 2> $stderrPath
    $exitCode = $LASTEXITCODE
    $text = ($output | Out-String).Trim()
    if ($AllowedExitCodes -notcontains $exitCode) {
        $stderr = if (Test-Path -LiteralPath $stderrPath) { (Get-Content -LiteralPath $stderrPath -Raw).Trim() } else { "" }
        throw "observatory $($CliArgs -join ' ') failed with exit ${exitCode}: $stderr"
    }
    if (-not $text) {
        throw "observatory $($CliArgs -join ' ') returned no JSON output"
    }
    try {
        return $text | ConvertFrom-Json -Depth 40
    } catch {
        throw "observatory $($CliArgs -join ' ') returned invalid JSON"
    }
}

function Wait-Http {
    param([Parameter(Mandatory = $true)][string]$Uri)

    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        try {
            $response = Invoke-WebRequest -UseBasicParsing -Uri $Uri -TimeoutSec 5
            if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 400) {
                return
            }
        } catch {
            # The service is expected to be unavailable during early startup.
        }
        Start-Sleep -Seconds 1
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "$Uri did not become ready within $TimeoutSeconds seconds"
}

function Get-FreeLoopbackPort {
    $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
    try {
        $listener.Start()
        return ([System.Net.IPEndPoint]$listener.LocalEndpoint).Port
    } finally {
        $listener.Stop()
    }
}

function Write-RuntimeCompose {
    param(
        [Parameter(Mandatory = $true)][int]$ApiPort,
        [Parameter(Mandatory = $true)][int]$GrafanaPort,
        [Parameter(Mandatory = $true)][int]$CollectorPort,
        [Parameter(Mandatory = $true)][int]$OtlpGrpcPort,
        [Parameter(Mandatory = $true)][int]$OtlpHttpPort
    )

    $rendered = $composeTemplate
    $rendered = $rendered.Replace('127.0.0.1:8787:8787', "127.0.0.1:${ApiPort}:8787")
    $rendered = $rendered.Replace('127.0.0.1:3000:3000', "127.0.0.1:${GrafanaPort}:3000")
    $rendered = $rendered.Replace('127.0.0.1:13133:13133', "127.0.0.1:${CollectorPort}:13133")
    $rendered = $rendered.Replace('127.0.0.1:4317:4317', "127.0.0.1:${OtlpGrpcPort}:4317")
    $rendered = $rendered.Replace('127.0.0.1:4318:4318', "127.0.0.1:${OtlpHttpPort}:4318")
    Set-Content -LiteralPath $runtimeComposeFile -Value $rendered -Encoding utf8 -NoNewline
    $script:ApiBase = "http://127.0.0.1:${ApiPort}"
    $script:CollectorBase = "http://127.0.0.1:${CollectorPort}"
    $env:OBSERVATORY_OTLP_GRPC_ENDPOINT = "http://127.0.0.1:${OtlpGrpcPort}"
    $env:OBSERVATORY_OTLP_HTTP_ENDPOINT = "http://127.0.0.1:${OtlpHttpPort}"
}

function Stop-IsolatedCompose {
    $composeEnv = Join-Path $statePath "compose.env"
    if (-not (Test-Path -LiteralPath $composeEnv) -or -not (Test-Path -LiteralPath $runtimeComposeFile)) {
        return
    }
    & docker compose --project-name $composeProject --env-file $composeEnv -f $runtimeComposeFile down --remove-orphans --volumes 2>&1 | Out-Null
}

function Get-Events {
    $response = Invoke-RestMethod -Uri "$script:ApiBase/v1/events?limit=256" -TimeoutSec 10
    return @($response.events)
}

function Get-EventAttributeValue {
    param(
        [Parameter(Mandatory = $true)][object]$Event,
        [Parameter(Mandatory = $true)][string]$Key
    )
    if ($null -eq $Event.attributes) { return $null }
    $property = $Event.attributes.PSObject.Properties[$Key]
    if ($null -eq $property) { return $null }
    return [string]$property.Value
}

function Get-GitStatus {
    if ([string]::IsNullOrWhiteSpace($gitRoot)) {
        throw "repository root is unavailable; refusing to claim repository cleanliness"
    }
    $tracked = @(& git -C $gitRoot ls-files --cached --others --exclude-standard 2>$null)
    if ($LASTEXITCODE -ne 0) { throw "git file inventory failed" }
    $ignored = @(& git -C $gitRoot ls-files --others --ignored --exclude-standard 2>$null)
    if ($LASTEXITCODE -ne 0) { throw "git ignored-file inventory failed" }
    $relativePaths = @($tracked + $ignored | Where-Object { $_ } | ForEach-Object { [string]$_ } | Sort-Object -Unique)
    $snapshot = foreach ($relativePath in $relativePaths) {
        $filePath = Join-Path $gitRoot ($relativePath -replace '/', '\')
        if (-not (Test-Path -LiteralPath $filePath -PathType Leaf)) {
            "${relativePath}|MISSING"
            continue
        }
        $hash = (Get-FileHash -LiteralPath $filePath -Algorithm SHA256).Hash.ToLowerInvariant()
        "${relativePath}|$hash"
    }
    return @($snapshot | Sort-Object)
}

function Invoke-ExplicitClient {
    $executable = $ClientCommand[0]
    $arguments = @()
    if ($ClientCommand.Count -gt 1) {
        $arguments = @($ClientCommand[1..($ClientCommand.Count - 1)])
    }
    $startInfo = [Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $executable
    $startInfo.WorkingDirectory = $projectPath
    $startInfo.UseShellExecute = $false
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    foreach ($argument in $arguments) {
        [void]$startInfo.ArgumentList.Add([string]$argument)
    }
    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    try {
        if (-not $process.Start()) {
            throw "could not start the explicit client command"
        }
        $stdoutTask = $process.StandardOutput.ReadToEndAsync()
        $stderrTask = $process.StandardError.ReadToEndAsync()
        $finished = $process.WaitForExit($TimeoutSeconds * 1000)
        if (-not $finished) {
            try { $process.Kill($true) } catch { }
            $process.WaitForExit()
        }
        $stdout = $stdoutTask.GetAwaiter().GetResult()
        $stderr = $stderrTask.GetAwaiter().GetResult()
        return [ordered]@{
            exit_code = if ($finished) { $process.ExitCode } else { $null }
            timed_out = -not $finished
            argument_count = $arguments.Count
            output_bytes = ([Text.Encoding]::UTF8.GetByteCount($stdout) + [Text.Encoding]::UTF8.GetByteCount($stderr))
        }
    } finally {
        $process.Dispose()
    }
}

$result = [ordered]@{
    schema = "observatory.provider-acceptance/v1"
    status = "pass"
    client = $Client
    state_dir = $statePath
    configuration_mode = if ($ApplyConfiguration) { "explicit-apply" } else { "plan-only" }
    inference_proxy = $false
    checks = [ordered]@{}
}

try {
    Assert-True (Test-Path -LiteralPath $composePath) "compose.yaml is missing"
    Assert-True ($ClientCommand.Count -gt 0) "an explicit client executable and argument list are required"
    Assert-True ($TimeoutSeconds -ge 10) "TimeoutSeconds must be at least 10"

    $rootOutput = & git -C $projectPath rev-parse --show-toplevel 2>$null
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace(($rootOutput | Out-String))) {
        throw "ProjectPath is not inside a readable Git repository; refusing to claim repository cleanliness"
    }
    $gitRoot = (Resolve-Path (($rootOutput | Out-String).Trim())).Path

    New-Item -ItemType Directory -Path $statePath -Force | Out-Null
    $runtimeComposeFile = Join-Path $statePath "compose.acceptance.yaml"
    $composeTemplate = Get-Content -LiteralPath $composePath -Raw
    $repoForCompose = $repositoryRoot.Replace('\', '/')
    $composeTemplate = $composeTemplate.Replace('context: .', "context: $repoForCompose")
    $composeTemplate = $composeTemplate.Replace('./deployment', "$repoForCompose/deployment")
    $composeTemplate = $composeTemplate.Replace('./dashboards', "$repoForCompose/dashboards")
    $composePath = [IO.Path]::GetFullPath($runtimeComposeFile)
    $apiPort = Get-FreeLoopbackPort
    $grafanaPort = Get-FreeLoopbackPort
    $collectorPort = Get-FreeLoopbackPort
    $otlpGrpcPort = Get-FreeLoopbackPort
    $otlpHttpPort = Get-FreeLoopbackPort
    Write-RuntimeCompose -ApiPort $apiPort -GrafanaPort $grafanaPort -CollectorPort $collectorPort -OtlpGrpcPort $otlpGrpcPort -OtlpHttpPort $otlpHttpPort
    $composePath = (Resolve-Path $runtimeComposeFile).Path
    $beforeGit = Get-GitStatus
    $install = Invoke-ObservatoryCli @("install", "--compose-file", $composePath)
    Assert-True ($install.outcome -eq "success") "install did not succeed"
    $result.checks.install = @{ status = "pass" }

    $configureArgs = @("configure", $Client)
    if ($ApplyConfiguration) { $configureArgs += "--apply" }
    $plan = Invoke-ObservatoryCli -CliArgs $configureArgs -AllowedExitCodes @(0, 5, 7)
    Assert-True ($plan.data.inference_proxy -eq $false) "client configuration plan enabled an inference proxy"
    $canonicalClient = [string]$plan.data.client
    $clientPlan = $null
    if ($plan.data.clients -and $plan.data.clients.PSObject.Properties[$canonicalClient]) {
        $clientPlan = $plan.data.clients.PSObject.Properties[$canonicalClient].Value
    }
    Assert-True ($null -ne $clientPlan) "configuration response did not identify the requested client"
    $expectedProvider = ([string]$clientPlan.provider).Trim().ToLowerInvariant()
    Assert-True (-not [string]::IsNullOrWhiteSpace($expectedProvider)) "configuration response did not identify the expected provider"
    $configurationConflicts = @($clientPlan.conflicts | Where-Object { $_ })
    if ($ApplyConfiguration) {
        Assert-True ($plan.outcome -eq "success" -and $clientPlan.applied -eq $true -and $configurationConflicts.Count -eq 0) "--ApplyConfiguration did not apply a conflict-free client configuration"
    }
    $expectedClientNames = @($canonicalClient, [string]$clientPlan.client) | Where-Object { $_ } | ForEach-Object { ([string]$_).Trim().ToLowerInvariant() } | Select-Object -Unique
    $expectedSourceNames = if ([string]::IsNullOrWhiteSpace($ExpectedSourceName)) { @($expectedClientNames) + @($expectedProvider) } else { @($ExpectedSourceName.Trim().ToLowerInvariant()) }
    $result.checks.configuration = @{ status = "pass"; outcome = $plan.outcome; inference_proxy = $plan.data.inference_proxy; expected_client = $expectedClientNames; expected_provider = $expectedProvider; expected_source = $expectedSourceNames; applied = [bool]$clientPlan.applied }

    $start = $null
    $startError = $null
    $startAttempts = 0
    $portRetryLimit = 3
    do {
        $startAttempts++
        try {
            $start = Invoke-ObservatoryCli @("start", "--compose-file", $composePath, "--api-url", "$script:ApiBase/readyz", "--collector-url", "$script:CollectorBase/", "--grafana-url", "http://127.0.0.1:${grafanaPort}/api/health")
            $startError = $null
        } catch {
            $start = $null
            $startError = $_.Exception.Message
        }
        if ($null -ne $start -and $start.outcome -eq "success") {
            break
        }
        $detail = if ($startError) { $startError } else { ($start | ConvertTo-Json -Depth 20 -Compress) }
        $portConflict = $detail -match '(?i)(port is already allocated|address already in use|bind .*failed|listen tcp .* failed|failed to bind|port.*allocated)'
        if (-not $portConflict -or $startAttempts -ge $portRetryLimit) {
            throw "Observatory start did not pass readiness: $detail"
        }
        Stop-IsolatedCompose
        $apiPort = Get-FreeLoopbackPort
        $grafanaPort = Get-FreeLoopbackPort
        $collectorPort = Get-FreeLoopbackPort
        $otlpGrpcPort = Get-FreeLoopbackPort
        $otlpHttpPort = Get-FreeLoopbackPort
        Write-RuntimeCompose -ApiPort $apiPort -GrafanaPort $grafanaPort -CollectorPort $collectorPort -OtlpGrpcPort $otlpGrpcPort -OtlpHttpPort $otlpHttpPort
    } while ($startAttempts -lt $portRetryLimit)
    $result.checks.start = @{ status = "pass"; attempts = $startAttempts; port_retry_limit = $portRetryLimit }
    Wait-Http "$script:ApiBase/readyz"
    $beforeEvents = Get-Events
    $beforeIds = @{}
    foreach ($event in $beforeEvents) {
        if ($null -ne $event.event_id) { $beforeIds[[string]$event.event_id] = $true }
    }

    $existingResourceAttributes = if ([string]::IsNullOrWhiteSpace($previousOtelResourceAttributes)) { @() } else { @($previousOtelResourceAttributes.TrimEnd(',')) }
    $env:OTEL_RESOURCE_ATTRIBUTES = (@($existingResourceAttributes + $acceptanceResourceAttribute) -join ",")
    $env:OTEL_EXPORTER_OTLP_ENDPOINT = $env:OBSERVATORY_OTLP_GRPC_ENDPOINT
    $env:OTEL_EXPORTER_OTLP_PROTOCOL = "grpc"
    $clientRun = Invoke-ExplicitClient
    $result.checks.client_process = $clientRun

    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    $afterEvents = @()
    do {
        $afterEvents = Get-Events
        $newEvents = @($afterEvents | Where-Object { $null -ne $_.event_id -and -not $beforeIds.ContainsKey([string]$_.event_id) })
        if ($newEvents.Count -gt 0) { break }
        Start-Sleep -Seconds 1
    } while ([DateTime]::UtcNow -lt $deadline)
    $newEvents = @($afterEvents | Where-Object { $null -ne $_.event_id -and -not $beforeIds.ContainsKey([string]$_.event_id) })
    $providerValues = @($newEvents | ForEach-Object { $_.llm.provider } | Where-Object { $_ -and $_ -ne "unknown" } | Select-Object -Unique)
    $modelValues = @($newEvents | ForEach-Object { $_.llm.model } | Where-Object { $_ -and $_ -ne "unknown" } | Select-Object -Unique)
    $identityEvents = @($newEvents | Where-Object {
        $eventProvider = ([string]$_.llm.provider).Trim().ToLowerInvariant()
        $eventClient = ([string]$_.llm.client).Trim().ToLowerInvariant()
        $sourceKind = ([string]$_.source.kind).Trim().ToLowerInvariant()
        $sourceName = ([string]$_.source.name).Trim().ToLowerInvariant()
        $eventProvider -eq $expectedProvider -and
            ($expectedClientNames -contains $eventClient) -and
            $sourceKind -and $sourceKind -ne "unknown" -and
            ($expectedSourceNames -contains $sourceName)
    })
    $causalEvents = @($identityEvents | Where-Object {
        (Get-EventAttributeValue -Event $_ -Key "llm.observatory.acceptance.run_id") -eq $acceptanceRunId
    })
    $identityClientValues = @($identityEvents | ForEach-Object { $_.llm.client } | Where-Object { $_ } | Select-Object -Unique)
    $identitySourceValues = @($identityEvents | ForEach-Object { "$($_.source.kind):$($_.source.name)" } | Select-Object -Unique)
    $identitySourceVersions = @($identityEvents | ForEach-Object { $_.source.version } | Where-Object { $_ -and $_ -ne "unknown" } | Select-Object -Unique)
    $sessionValues = @($newEvents | ForEach-Object { $_.execution.session_id } | Where-Object { $_ } | Select-Object -Unique)
    $eventTypes = @($newEvents | ForEach-Object { $_.event_type } | Where-Object { $_ } | Select-Object -Unique)
    $eventJson = ($newEvents | ConvertTo-Json -Depth 40)
    $privacyViolations = @()
    foreach ($pattern in @(
        '(?i)bearer\s+[A-Za-z0-9._~+/=-]{8,}',
        '(?i)"(api[_-]?key|authorization|access[_-]?token|client[_-]?secret|password|private[_-]?key)"\s*:',
        '(?i)"(prompt|completion|tool[_-]?(arguments?|input|output|result))"\s*:'
    )) {
        if ($eventJson -match $pattern) { $privacyViolations += $pattern }
    }
    $contentCaptureValues = @($newEvents | ForEach-Object { $_.provenance.content_capture } | Select-Object -Unique)
    $afterGit = Get-GitStatus
    $repositoryUnchanged = (($beforeGit -join "`n") -eq ($afterGit -join "`n"))

    $result.checks.telemetry = [ordered]@{
        status = if ($causalEvents.Count -gt 0 -and $modelValues.Count -gt 0) { "pass" } else { "fail" }
        new_events = $newEvents.Count
        providers = $providerValues
        models = $modelValues
        identity_events = $identityEvents.Count
        causal_events = $causalEvents.Count
        acceptance_run_id = $acceptanceRunId
        clients = $identityClientValues
        sources = $identitySourceValues
        source_versions = $identitySourceVersions
        event_types = $eventTypes
        session_ids_observed = $sessionValues.Count
        content_capture = $contentCaptureValues
    }
    $result.checks.privacy = @{ status = if ($privacyViolations.Count -eq 0) { "pass" } else { "fail" }; violations = $privacyViolations }
    $result.checks.repository_contamination = @{ status = if ($repositoryUnchanged) { "pass" } else { "fail" }; unchanged = $repositoryUnchanged }

    $failed = ($clientRun.timed_out -or $clientRun.exit_code -ne 0 -or $newEvents.Count -eq 0 -or $identityEvents.Count -eq 0 -or $causalEvents.Count -eq 0 -or $modelValues.Count -eq 0 -or $privacyViolations.Count -gt 0 -or -not $repositoryUnchanged)
    if ($failed) {
        $result.status = "fail"
    }
} catch {
    $result.status = "fail"
    $result.error = $_.Exception.Message
    $result.error_location = $_.InvocationInfo.PositionMessage
} finally {
    if ($ApplyConfiguration -and $canonicalClient) {
        try {
            $remove = Invoke-ObservatoryCli -CliArgs @("configure", $canonicalClient, "--remove", "--apply") -AllowedExitCodes @(0, 6)
            $result.checks.configuration_cleanup = @{ status = if ($remove.outcome -eq "success") { "pass" } else { "conflict" }; outcome = $remove.outcome; data = $remove.data }
            if ($remove.outcome -ne "success") {
                $result.status = "fail"
                $result.cleanup_warning = "$canonicalClient configuration cleanup returned $($remove.outcome)"
            }
        } catch {
            $result.status = "fail"
            $result.cleanup_warning = "client configuration cleanup failed: $($_.Exception.Message)"
        }
    }
    try {
        if (Test-Path -LiteralPath (Join-Path $statePath "config.json")) {
            Invoke-ObservatoryCli -CliArgs @("stop") -AllowedExitCodes @(0, 5) | Out-Null
            $composeEnv = Join-Path $statePath "compose.env"
            $cleanupOutput = & docker compose --project-name $composeProject --env-file $composeEnv -f $composePath down --remove-orphans --volumes 2>&1
            if ($LASTEXITCODE -ne 0) {
                throw "Compose cleanup failed: $(($cleanupOutput | Out-String).Trim())"
            }
        }
    } catch {
        $result.status = "fail"
        $result.cleanup_warning = $_.Exception.Message
    }
    if ($temporaryState -and -not $KeepState -and (Test-Path -LiteralPath $statePath)) {
        Remove-Item -LiteralPath $statePath -Recurse -Force
    }
    if ($null -eq $previousPythonPath) { Remove-Item Env:PYTHONPATH -ErrorAction SilentlyContinue } else { $env:PYTHONPATH = $previousPythonPath }
    if ($null -eq $previousOtlpGrpcEndpoint) { Remove-Item Env:OBSERVATORY_OTLP_GRPC_ENDPOINT -ErrorAction SilentlyContinue } else { $env:OBSERVATORY_OTLP_GRPC_ENDPOINT = $previousOtlpGrpcEndpoint }
    if ($null -eq $previousOtlpHttpEndpoint) { Remove-Item Env:OBSERVATORY_OTLP_HTTP_ENDPOINT -ErrorAction SilentlyContinue } else { $env:OBSERVATORY_OTLP_HTTP_ENDPOINT = $previousOtlpHttpEndpoint }
    if ($null -eq $previousOtelResourceAttributes) { Remove-Item Env:OTEL_RESOURCE_ATTRIBUTES -ErrorAction SilentlyContinue } else { $env:OTEL_RESOURCE_ATTRIBUTES = $previousOtelResourceAttributes }
    if ($null -eq $previousOtelExporterEndpoint) { Remove-Item Env:OTEL_EXPORTER_OTLP_ENDPOINT -ErrorAction SilentlyContinue } else { $env:OTEL_EXPORTER_OTLP_ENDPOINT = $previousOtelExporterEndpoint }
    if ($null -eq $previousOtelExporterProtocol) { Remove-Item Env:OTEL_EXPORTER_OTLP_PROTOCOL -ErrorAction SilentlyContinue } else { $env:OTEL_EXPORTER_OTLP_PROTOCOL = $previousOtelExporterProtocol }
    if ($null -eq $previousComposeProject) { Remove-Item Env:COMPOSE_PROJECT_NAME -ErrorAction SilentlyContinue } else { $env:COMPOSE_PROJECT_NAME = $previousComposeProject }
}

$result | ConvertTo-Json -Depth 40
if ($result.status -ne "pass") {
    exit 1
}
