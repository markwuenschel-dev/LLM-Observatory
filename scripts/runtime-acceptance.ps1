[CmdletBinding()]
param(
    [string]$StateDir,
    [string]$Python = "python",
    [int]$TimeoutSeconds = 180,
    [switch]$KeepState,
    [switch]$KeepVolumes
)

$ErrorActionPreference = "Stop"

$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$composeFile = Join-Path $repositoryRoot "compose.yaml"
$fixtureFile = Join-Path $repositoryRoot "examples\synthetic-events.jsonl"
$temporaryState = [string]::IsNullOrWhiteSpace($StateDir)
$composeProject = "llm-observatory-acceptance-$([Guid]::NewGuid().ToString('N').Substring(0, 12))"
$acceptanceApiImage = "llm-observatory-api:acceptance-$([Guid]::NewGuid().ToString('N').Substring(0, 12))"
$previousProjectName = $env:COMPOSE_PROJECT_NAME
$previousPythonPath = $env:PYTHONPATH
$previousApiImage = $env:OBSERVATORY_API_IMAGE
$script:ApiBase = "http://127.0.0.1:8787"
$script:GrafanaBase = "http://127.0.0.1:3000"
$script:CollectorBase = "http://127.0.0.1:13133"
$script:OtlpHttpBase = "http://127.0.0.1:4318"

if ($temporaryState) {
    $StateDir = Join-Path ([IO.Path]::GetTempPath()) "llm-observatory-acceptance-$([Guid]::NewGuid().ToString('N'))"
}
$statePath = [IO.Path]::GetFullPath($StateDir)

function Invoke-ObservatoryCli {
    param(
        [Parameter(Mandatory)][string[]]$CliArgs,
        [int[]]$AllowedExitCodes = @(0)
    )

    $stderrPath = Join-Path $statePath ".cli-stderr"
    $output = & $Python -m observatory.cli --json --timeout $TimeoutSeconds --state-dir $statePath @CliArgs 2> $stderrPath
    $exitCode = $LASTEXITCODE
    $text = ($output | Out-String).Trim()
    $stderr = ""
    if (Test-Path -LiteralPath $stderrPath) {
        $stderrContent = Get-Content -LiteralPath $stderrPath -Raw
        if ($null -ne $stderrContent) {
            $stderr = $stderrContent.Trim()
        }
    }
    if ($AllowedExitCodes -notcontains $exitCode) {
        throw "observatory $($CliArgs -join ' ') failed with exit $exitCode. stdout: $text stderr: $stderr"
    }
    if (-not $text) {
        throw "observatory $($CliArgs -join ' ') returned no JSON output"
    }
    try {
        return $text | ConvertFrom-Json -Depth 40
    } catch {
        throw "observatory $($CliArgs -join ' ') returned invalid JSON: $text"
    }
}

function Invoke-Compose {
    param([Parameter(Mandatory)][string[]]$ComposeArgs)

    $output = & docker compose --project-name $composeProject --env-file (Join-Path $statePath "compose.env") -f $composeFile @ComposeArgs 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose $($ComposeArgs -join ' ') failed: $(($output | Out-String).Trim())"
    }
    return ($output | Out-String).Trim()
}

function Wait-Http {
    param([Parameter(Mandatory)][string]$Uri)

    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    $lastError = "no response"
    do {
        try {
            $response = Invoke-WebRequest -UseBasicParsing -Uri $Uri -TimeoutSec 5
            if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 400) {
                return $response
            }
            $lastError = "HTTP $($response.StatusCode)"
        } catch {
            $lastError = $_.Exception.Message
        }
        Start-Sleep -Seconds 1
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "$Uri did not become ready within $TimeoutSeconds seconds: $lastError"
}

function Invoke-InferenceSentinel {
    $code = @'
import http.server
import threading
import urllib.request

class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length", "0")))
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ok":true}')

    def log_message(self, *_args):
        return

server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
threading.Thread(target=server.serve_forever, daemon=True).start()
try:
    request = urllib.request.Request(
        f"http://127.0.0.1:{server.server_port}/v1/chat/completions",
        data=b'{"model":"sentinel"}',
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        if response.status != 200 or response.read() != b'{"ok":true}':
            raise SystemExit(2)
finally:
    server.shutdown()
    server.server_close()
print("inference-sentinel")
'@
    $output = & $Python -c $code 2>&1
    $exitCode = $LASTEXITCODE
    $text = ($output | Out-String).Trim()
    Assert-True ($exitCode -eq 0 -and $text -eq "inference-sentinel") "unmanaged inference sentinel was affected by an Observatory failure"
    return [ordered]@{ exit_code = $exitCode; output = "inference-sentinel" }
}

function Get-JsonEndpoint {
    param([Parameter(Mandatory)][string]$Uri)
    return (Invoke-RestMethod -Uri $Uri -TimeoutSec 10)
}

function Get-Events {
    return @((Get-JsonEndpoint "$script:ApiBase/v1/events?limit=256").events)
}

function Assert-Equal {
    param([Parameter(Mandatory)]$Actual, [Parameter(Mandatory)]$Expected, [Parameter(Mandatory)][string]$Message)
    if ($Actual -ne $Expected) {
        throw "$Message. expected '$Expected', got '$Actual'"
    }
}

function Assert-True {
    param([Parameter(Mandatory)][bool]$Condition, [Parameter(Mandatory)][string]$Message)
    if (-not $Condition) {
        throw $Message
    }
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

function Get-GrafanaHeaders {
    $passwordPath = Join-Path $statePath "secrets\grafana_admin_password"
    $password = (Get-Content -LiteralPath $passwordPath -Raw).Trim()
    $encoded = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("admin:$password"))
    return @{ Authorization = "Basic $encoded" }
}

function Wait-GrafanaApi {
    $headers = Get-GrafanaHeaders
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    $lastError = "no authenticated response"
    do {
        try {
            $response = Invoke-WebRequest -UseBasicParsing -Headers $headers -Uri "$script:GrafanaBase/api/datasources" -TimeoutSec 10
            if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 300) {
                return $headers
            }
            $lastError = "HTTP $($response.StatusCode)"
        } catch {
            $lastError = $_.Exception.Message
        }
        Start-Sleep -Seconds 1
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Grafana authenticated API did not become ready within $TimeoutSeconds seconds: $lastError"
}

function Wait-GrafanaPrometheusMetric {
    param([Parameter(Mandatory)][hashtable]$Headers)

    $query = [Uri]::EscapeDataString("sum(observatory_events_by_context_total)")
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    $lastError = "no Prometheus sample"
    do {
        try {
            $response = Invoke-RestMethod -Headers $Headers -Uri "$script:GrafanaBase/api/datasources/uid/prometheus/resources/api/v1/query?query=$query" -TimeoutSec 10
            $results = @($response.data.result)
            if ($results.Count -gt 0 -and @($results[0].value).Count -ge 2) {
                return [int]$results[0].value[1]
            }
            $lastError = "Prometheus returned no matching sample"
        } catch {
            $lastError = $_.Exception.Message
        }
        Start-Sleep -Seconds 1
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Grafana Prometheus query did not become ready within $TimeoutSeconds seconds: $lastError"
}

function Invoke-GrafanaEventTimeRangeProbe {
    param([Parameter(Mandatory)][hashtable]$Headers)

    $now = [DateTimeOffset]::UtcNow
    $start = $now.AddHours(-24).ToUnixTimeSeconds()
    $end = $now.ToUnixTimeSeconds()
    $query = [Uri]::EscapeDataString("sum(observatory_events_by_context_total)")
    $response = Invoke-RestMethod -Headers $Headers -Uri "$script:GrafanaBase/api/datasources/proxy/uid/prometheus/api/v1/query_range?query=$query&start=$start&end=$end&step=300" -TimeoutSec 10
    Assert-True ($response.status -eq "success") "Observatory Events query_range did not return success"
    Assert-True ($response.data.resultType -eq "matrix") "Observatory Events query_range did not return a matrix"
    $results = @($response.data.result)
    Assert-True ($results.Count -gt 0 -and @($results[0].values).Count -gt 0) "Observatory Events query_range returned no event-time samples"
    return [pscustomobject]@{ status = "pass"; result_type = $response.data.resultType; samples = @($results[0].values).Count }
}

function Invoke-GrafanaTempoSessionProbe {
    param([Parameter(Mandatory)][hashtable]$Headers)

    $traceql = [Uri]::EscapeDataString('{ span.llm.observatory.session.id = "runtime-session" }')
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    $traces = @()
    do {
        try {
            $response = Invoke-RestMethod -Headers $Headers -Uri "$script:GrafanaBase/api/datasources/proxy/uid/tempo/api/search?limit=20&q=$traceql" -TimeoutSec 10
            $traces = @($response.traces)
            if ($traces.Count -gt 0) { break }
        } catch { }
        Start-Sleep -Seconds 1
    } while ([DateTime]::UtcNow -lt $deadline)
    Assert-True ($traces.Count -gt 0) "Tempo TraceQL session filter returned no traces"
    return [pscustomobject]@{ status = "pass"; traces = $traces.Count; session_id = "runtime-session" }
}

function Wait-GrafanaCollectorSelfMetric {
    param([Parameter(Mandatory)][hashtable]$Headers)

    $query = [Uri]::EscapeDataString("otelcol_process_uptime")
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    $lastError = "no Collector self-metric sample"
    do {
        try {
            $response = Invoke-RestMethod -Headers $Headers -Uri "$script:GrafanaBase/api/datasources/proxy/uid/system-prometheus/api/v1/query?query=$query" -TimeoutSec 10
            $results = @($response.data.result)
            if ($results.Count -gt 0 -and @($results[0].value).Count -ge 2) {
                return $response
            }
            $lastError = "Prometheus returned no Collector self-metric sample"
        } catch {
            $lastError = $_.Exception.Message
        }
        Start-Sleep -Seconds 1
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Collector self-metrics did not become queryable within $TimeoutSeconds seconds: $lastError"
}

function Invoke-GrafanaDashboardQueryProbe {
    param(
        [Parameter(Mandatory)][hashtable]$Headers,
        [Parameter(Mandatory)][string[]]$DashboardUids
    )

    $probed = @()
    $now = [DateTimeOffset]::UtcNow
    $start = $now.AddHours(-24).ToUnixTimeMilliseconds() * 1000000
    $end = $now.ToUnixTimeMilliseconds() * 1000000
    $dashboardRoot = Join-Path $repositoryRoot "dashboards"
    foreach ($localPath in (Get-ChildItem -LiteralPath $dashboardRoot -Filter "*.json")) {
        $definition = Get-Content -LiteralPath $localPath.FullName -Raw | ConvertFrom-Json -Depth 40
        $uid = [string]$definition.uid
        if ($DashboardUids -notcontains $uid) { continue }
        foreach ($panel in @($definition.panels)) {
            $datasourceUid = [string]$panel.datasource.uid
            foreach ($target in @($panel.targets)) {
                if ($datasourceUid -in @("prometheus", "system-prometheus") -and $target.expr) {
                    $expr = [regex]::Replace([string]$target.expr, '\$[A-Za-z_][A-Za-z0-9_]*', '.*')
                    $query = [Uri]::EscapeDataString($expr)
                    $response = Invoke-RestMethod -Headers $Headers -Uri "$script:GrafanaBase/api/datasources/proxy/uid/$datasourceUid/api/v1/query?query=$query" -TimeoutSec 10
                    Assert-True ($response.status -eq "success") "Prometheus query failed for dashboard '$uid' panel '$($panel.title)'"
                    Assert-True ($null -ne $response.data -and $null -ne $response.data.resultType -and $null -ne $response.data.result) "Prometheus query returned an invalid response for dashboard '$uid' panel '$($panel.title)'"
                    $metricResults = @($response.data.result)
                    $probed += [pscustomobject]@{ dashboard = $uid; signal = "metrics"; result_type = [string]$response.data.resultType; result_count = $metricResults.Count }
                } elseif ($datasourceUid -eq "loki" -and $target.expr) {
                    $query = [Uri]::EscapeDataString([string]$target.expr)
                    $response = Invoke-RestMethod -Headers $Headers -Uri "$script:GrafanaBase/api/datasources/proxy/uid/loki/loki/api/v1/query_range?query=$query&start=$start&end=$end&limit=20&direction=backward" -TimeoutSec 10
                    Assert-True ($response.status -eq "success" -and $null -ne $response.data) "Loki query failed for dashboard '$uid' panel '$($panel.title)'"
                    $logResults = @($response.data.result)
                    $probed += [pscustomobject]@{ dashboard = $uid; signal = "logs"; result_count = $logResults.Count }
                } elseif ($datasourceUid -eq "tempo") {
                    $traceQuery = if ([string]::IsNullOrWhiteSpace([string]$target.query)) { "{}" } else { [string]$target.query }
                    $traceQuery = [regex]::Replace($traceQuery, '\$[A-Za-z_][A-Za-z0-9_]*', '.*')
                    $encodedTraceQuery = [Uri]::EscapeDataString($traceQuery)
                    $response = Invoke-RestMethod -Headers $Headers -Uri "$script:GrafanaBase/api/datasources/proxy/uid/tempo/api/search?q=$encodedTraceQuery&limit=20" -TimeoutSec 10
                    Assert-True ($null -ne $response -and $null -ne $response.traces) "Tempo query failed for dashboard '$uid' panel '$($panel.title)'"
                    Assert-True (@($response.traces).Count -gt 0) "Tempo query returned no traces for dashboard '$uid' panel '$($panel.title)'"
                    $probed += [pscustomobject]@{ dashboard = $uid; signal = "traces"; query = $traceQuery; result_count = @($response.traces).Count }
                }
            }
        }
    }
    Assert-True ($probed.Count -gt 0) "No dashboard targets were exercised"
    foreach ($uid in $DashboardUids) {
        $dashboardProbes = @($probed | Where-Object { $_.dashboard -eq $uid })
        Assert-True ($dashboardProbes.Count -gt 0) "Dashboard '$uid' had no executable targets"
        Assert-True (@($dashboardProbes | Where-Object { [int]$_.result_count -gt 0 }).Count -gt 0) "Dashboard '$uid' had no data-bearing target"
    }
    return @($probed)
}

function Invoke-GrafanaDashboardFilterProbe {
    param([Parameter(Mandatory)][hashtable]$Headers)

    $queries = [ordered]@{
        project_alpha_events = 'sum(observatory_events_by_context_total{project="repo:alpha"})'
        project_beta_events = 'sum(observatory_events_by_context_total{project="repo:beta"})'
        project_alpha_input_tokens = 'sum(observatory_input_tokens_by_context_total{project="repo:alpha",event_type="model.operation"})'
        project_alpha_retries = 'sum(observatory_retries_by_context_total{project="repo:alpha"})'
        project_alpha_execution = 'sum(observatory_events_by_execution_total{project="repo:alpha",skill="repo-quality"})'
        project_alpha_workflow = 'sum(observatory_events_by_workflow_total{project="repo:alpha",workflow="workflow-implementation"})'
        project_alpha_agent = 'sum(observatory_events_by_agent_total{project="repo:alpha",agent="agent-parent"})'
        project_alpha_outcome = 'sum(observatory_outcomes_by_kind_status_total{project="repo:alpha",kind="tests",status="passed"})'
    }
    $values = [ordered]@{}
    foreach ($entry in $queries.GetEnumerator()) {
        $query = [Uri]::EscapeDataString([string]$entry.Value)
        $response = Invoke-RestMethod -Headers $Headers -Uri "$script:GrafanaBase/api/datasources/proxy/uid/prometheus/api/v1/query?query=$query" -TimeoutSec 10
        Assert-True ($response.status -eq "success") "Grafana filter query '$($entry.Key)' failed"
        $results = @($response.data.result)
        Assert-True ($results.Count -gt 0 -and @($results[0].value).Count -ge 2) "Grafana filter query '$($entry.Key)' returned no sample"
        $values[$entry.Key] = [double]$results[0].value[1]
        Assert-True ($values[$entry.Key] -gt 0) "Grafana filter query '$($entry.Key)' returned zero"
    }
    Assert-True ($values.project_alpha_events -ne $values.project_beta_events) "Project filter did not distinguish the synthetic alpha and beta projects"
    return $values
}

function Send-SyntheticOtlpTrace {
    param(
        [string]$TraceId = "0123456789abcdef0123456789abcdef",
        [string]$SpanId = "0123456789abcdef",
        [switch]$IncludePrivacyCanary,
        [switch]$ProjectRootOnly,
        [switch]$ProjectWorkingDirectoryOnly
    )

    $now = [DateTimeOffset]::UtcNow
    $start = $now.ToUnixTimeMilliseconds() * 1000000
    $end = $start + 25000000
    $attributes = @(
        @{ key = "gen_ai.provider.name"; value = @{ stringValue = "runtime-provider" } },
        @{ key = "gen_ai.request.model"; value = @{ stringValue = "runtime-model" } },
        @{ key = "gen_ai.usage.input_tokens"; value = @{ intValue = "10" } },
        @{ key = "gen_ai.usage.output_tokens"; value = @{ intValue = "5" } },
        @{ key = "llm.observatory.session.id"; value = @{ stringValue = "runtime-session" } },
        @{ key = "llm.observatory.evidence.source"; value = @{ stringValue = "runtime-fixture" } }
    )
    if ($IncludePrivacyCanary) {
        $attributes += @(
            @{ key = "gen_ai.prompt"; value = @{ stringValue = "RUNTIME_PROMPT_CANARY" } },
            @{ key = "gen_ai.completion"; value = @{ stringValue = "RUNTIME_COMPLETION_CANARY" } },
            @{ key = "api_key"; value = @{ stringValue = "RUNTIME_API_KEY_CANARY" } },
            @{ key = "provider_secret"; value = @{ stringValue = "RUNTIME_SECRET_CANARY" } },
            @{ key = "llm.observatory.raw_api_body"; value = @{ stringValue = "RUNTIME_BODY_CANARY" } },
            @{ key = "authToken"; value = @{ stringValue = "RUNTIME_AUTH_TOKEN_CANARY" } },
            @{ key = "clientSecret"; value = @{ stringValue = "RUNTIME_CLIENT_SECRET_CANARY" } },
            @{ key = "promptText"; value = @{ stringValue = "RUNTIME_PROMPT_TEXT_CANARY" } },
            @{ key = "toolArguments"; value = @{ stringValue = "RUNTIME_TOOL_ARGUMENTS_CANARY" } },
            @{ key = "unknownSensitive"; value = @{ stringValue = "RUNTIME_UNKNOWN_SENSITIVE_CANARY" } },
            @{ key = "runtime.unknown.attribute"; value = @{ stringValue = "RUNTIME_UNKNOWN_ATTRIBUTE_CANARY" } },
            @{ key = "llm.observatory.project.root"; value = @{ stringValue = "RUNTIME_PROJECT_ROOT_CANARY" } },
            @{ key = "llm.observatory.project.remote"; value = @{ stringValue = "RUNTIME_PROJECT_REMOTE_CANARY" } },
            @{ key = "process.environment"; value = @{ stringValue = "RUNTIME_PROCESS_ENVIRONMENT_CANARY" } },
            @{ key = "user.email"; value = @{ stringValue = "RUNTIME_USER_EMAIL_CANARY" } },
            @{ key = "environment"; value = @{ stringValue = "RUNTIME_ENVIRONMENT_CANARY" } }
        )
    }
    $resourceAttributes = @(
        @{ key = "service.name"; value = @{ stringValue = "runtime-acceptance" } },
        @{ key = "llm.observatory.client"; value = @{ stringValue = "runtime-acceptance" } }
    )
    if ($ProjectRootOnly) {
        $resourceAttributes += @{ key = "llm.observatory.project.root"; value = @{ stringValue = "C:\\Observed\\ProjectRoot" } }
    } elseif ($ProjectWorkingDirectoryOnly) {
        $resourceAttributes += @{ key = "process.cwd"; value = @{ stringValue = "C:\\Observed\\WorkingDirectory" } }
    } else {
        $resourceAttributes += @{ key = "llm.observatory.project.id"; value = @{ stringValue = "runtime-acceptance-project" } }
    }
    $payload = @{
        resourceSpans = @(@{
            resource = @{ attributes = $resourceAttributes }
            scopeSpans = @(@{
                scope = @{ name = "runtime-acceptance"; version = "1.0" }
                spans = @(@{
                    traceId = $TraceId
                    spanId = $SpanId
                    name = "gen_ai.chat"
                    startTimeUnixNano = "$start"
                    endTimeUnixNano = "$end"
                    attributes = $attributes
                    status = @{ code = "STATUS_CODE_OK" }
                })
            })
        })
    } | ConvertTo-Json -Depth 30
    return Invoke-WebRequest -Method Post -Uri "$script:OtlpHttpBase/v1/traces" -ContentType "application/json" -Body $payload -TimeoutSec 10
}

function Send-SyntheticOtlpLog {
    param([switch]$IncludePrivacyCanary)

    $now = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() * 1000000
    $body = "RUNTIME_LOG_CANARY"
    $attributes = @(@{ key = "runtime.log.kind"; value = @{ stringValue = "recovery-fixture" } })
    if ($IncludePrivacyCanary) {
        $body = "RUNTIME_LOG_PROMPT_CANARY"
        $attributes += @(
            @{ key = "gen_ai.prompt"; value = @{ stringValue = "RUNTIME_LOG_PROMPT_ATTRIBUTE_CANARY" } },
            @{ key = "api_key"; value = @{ stringValue = "RUNTIME_LOG_API_KEY_CANARY" } },
            @{ key = "process.environment"; value = @{ stringValue = "RUNTIME_LOG_PROCESS_ENVIRONMENT_CANARY" } },
            @{ key = "user.email"; value = @{ stringValue = "RUNTIME_LOG_USER_EMAIL_CANARY" } }
        )
    }
    $payload = @{
        resourceLogs = @(@{
            resource = @{ attributes = @(
                @{ key = "service.name"; value = @{ stringValue = "runtime-acceptance" } },
                @{ key = "llm.observatory.client"; value = @{ stringValue = "runtime-acceptance" } }
            ) }
            scopeLogs = @(@{
                scope = @{ name = "runtime-acceptance"; version = "1.0" }
                logRecords = @(@{
                    timeUnixNano = "$now"
                    severityText = "INFO"
                    body = @{ stringValue = $body }
                    attributes = $attributes
                })
            })
        })
    } | ConvertTo-Json -Depth 30
    return Invoke-WebRequest -Method Post -Uri "$script:OtlpHttpBase/v1/logs" -ContentType "application/json" -Body $payload -TimeoutSec 10
}

function Send-ClaudeShapedOtlpLog {
    $now = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() * 1000000
    $payload = @{
        resourceLogs = @(@{
            resource = @{ attributes = @(
                @{ key = "service.name"; value = @{ stringValue = "claude-code" } },
                @{ key = "service.version"; value = @{ stringValue = "2.1.226" } }
            ) }
            scopeLogs = @(@{
                scope = @{ name = "com.anthropic.claude_code" }
                logRecords = @(@{
                    timeUnixNano = "$now"
                    severityText = "INFO"
                    attributes = @(
                        @{ key = "event.name"; value = @{ stringValue = "api_request" } },
                        @{ key = "session.id"; value = @{ stringValue = "runtime-claude-session" } },
                        @{ key = "model"; value = @{ stringValue = "runtime-claude-model" } },
                        @{ key = "input_tokens"; value = @{ intValue = "11" } },
                        @{ key = "output_tokens"; value = @{ intValue = "7" } },
                        @{ key = "cache_read_tokens"; value = @{ intValue = "2" } },
                        @{ key = "cache_creation_tokens"; value = @{ intValue = "3" } },
                        @{ key = "cost_usd"; value = @{ doubleValue = 0.001 } },
                        @{ key = "duration_ms"; value = @{ intValue = "19" } },
                        @{ key = "ttft_ms"; value = @{ intValue = "5" } },
                        @{ key = "success"; value = @{ boolValue = $true } }
                    )
                })
            })
        })
    } | ConvertTo-Json -Depth 30
    return Invoke-WebRequest -Method Post -Uri "$script:OtlpHttpBase/v1/logs" -ContentType "application/json" -Body $payload -TimeoutSec 10
}

function Send-SyntheticOtlpMetric {
    param([switch]$IncludePrivacyCanary)

    $now = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() * 1000000
    $dataPointAttributes = @()
    if ($IncludePrivacyCanary) {
        $dataPointAttributes = @(
            @{ key = "api_key"; value = @{ stringValue = "RUNTIME_METRIC_API_KEY_CANARY" } },
            @{ key = "process.environment"; value = @{ stringValue = "RUNTIME_METRIC_PROCESS_ENVIRONMENT_CANARY" } },
            @{ key = "user.email"; value = @{ stringValue = "RUNTIME_METRIC_USER_EMAIL_CANARY" } }
        )
    }
    $payload = @{
        resourceMetrics = @(@{
            resource = @{ attributes = @(
                @{ key = "service.name"; value = @{ stringValue = "runtime-acceptance" } },
                @{ key = "llm.observatory.client"; value = @{ stringValue = "runtime-acceptance" } }
            ) }
            scopeMetrics = @(@{
                scope = @{ name = "runtime-acceptance"; version = "1.0" }
                metrics = @(@{
                    name = "runtime_privacy_metric"
                    gauge = @{ dataPoints = @(@{
                        asDouble = 1
                        timeUnixNano = "$now"
                        attributes = $dataPointAttributes
                    }) }
                })
            })
        })
    } | ConvertTo-Json -Depth 30
    return Invoke-WebRequest -Method Post -Uri "$script:OtlpHttpBase/v1/metrics" -ContentType "application/json" -Body $payload -TimeoutSec 10
}

$result = [ordered]@{
    schema = "observatory.runtime-acceptance/v1"
    status = "pass"
    project = $composeProject
    state_dir = $statePath
    checks = [ordered]@{}
}

try {
    Assert-True (Test-Path -LiteralPath $composeFile) "compose.yaml is missing"
    Assert-True (Test-Path -LiteralPath $fixtureFile) "synthetic fixture is missing"

    $engineVersion = & docker info --format '{{.ServerVersion}}' 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "Docker engine is unavailable. Start Docker Desktop in Linux-container mode before running this gate: $(($engineVersion | Out-String).Trim())"
    }
    $result.checks.engine = @{ status = "pass"; version = ($engineVersion | Out-String).Trim() }

    New-Item -ItemType Directory -Path $statePath -Force | Out-Null
    $apiPort = Get-FreeLoopbackPort
    $grafanaPort = Get-FreeLoopbackPort
    $collectorPort = Get-FreeLoopbackPort
    $otlpGrpcPort = Get-FreeLoopbackPort
    $otlpHttpPort = Get-FreeLoopbackPort
    $runtimeComposeFile = Join-Path $statePath "compose.acceptance.yaml"
    $composeText = Get-Content -LiteralPath $composeFile -Raw
    $repoForCompose = $repositoryRoot.Replace('\', '/')
    $composeText = $composeText.Replace('context: .', "context: $repoForCompose")
    $composeText = $composeText.Replace('./deployment', "$repoForCompose/deployment")
    $composeText = $composeText.Replace('./dashboards', "$repoForCompose/dashboards")
    $composeText = $composeText.Replace('127.0.0.1:8787:8787', "127.0.0.1:${apiPort}:8787")
    $composeText = $composeText.Replace('127.0.0.1:3000:3000', "127.0.0.1:${grafanaPort}:3000")
    $composeText = $composeText.Replace('127.0.0.1:13133:13133', "127.0.0.1:${collectorPort}:13133")
    $composeText = $composeText.Replace('127.0.0.1:4317:4317', "127.0.0.1:${otlpGrpcPort}:4317")
    $composeText = $composeText.Replace('127.0.0.1:4318:4318', "127.0.0.1:${otlpHttpPort}:4318")
    Set-Content -LiteralPath $runtimeComposeFile -Value $composeText -Encoding utf8 -NoNewline
    $composeFile = $runtimeComposeFile
    $script:ApiBase = "http://127.0.0.1:${apiPort}"
    $script:GrafanaBase = "http://127.0.0.1:${grafanaPort}"
    $script:CollectorBase = "http://127.0.0.1:${collectorPort}"
    $script:OtlpHttpBase = "http://127.0.0.1:${otlpHttpPort}"
    $env:COMPOSE_PROJECT_NAME = $composeProject
    $env:PYTHONPATH = Join-Path $repositoryRoot "src"
    $env:OBSERVATORY_API_IMAGE = $acceptanceApiImage

    $install = Invoke-ObservatoryCli @("install", "--compose-file", $composeFile)
    Assert-Equal $install.outcome "success" "install did not succeed"
    $result.checks.install = @{ status = "pass"; changed = $install.data.changed }

    Invoke-Compose @("build", "--pull=false", "observatory-api") | Out-Null
    $result.checks.api_build = @{ status = "pass"; image = $acceptanceApiImage }

    $doctor = Invoke-ObservatoryCli -CliArgs @("doctor", "--compose-file", $composeFile) -AllowedExitCodes @(0, 5)
    Assert-True ($doctor.outcome -in @("success", "degraded")) "doctor returned an unexpected outcome"
    $result.checks.doctor = @{ status = "pass"; outcome = $doctor.outcome }

    $start = Invoke-ObservatoryCli @("start", "--compose-file", $composeFile, "--api-url", "$script:ApiBase/readyz", "--collector-url", "$script:CollectorBase/", "--grafana-url", "$script:GrafanaBase/api/health")
    Assert-Equal $start.outcome "success" "start did not pass readiness"
    $result.checks.start = @{ status = "pass"; readiness = $start.data.readiness }

    Wait-Http "$script:ApiBase/readyz" | Out-Null
    Wait-Http "$script:CollectorBase/" | Out-Null
    Wait-Http "$script:GrafanaBase/api/health" | Out-Null
    $health = Get-JsonEndpoint "$script:ApiBase/healthz"
    Assert-Equal $health.status "ok" "normalizer health is not ok"
    $metrics = (Invoke-WebRequest -UseBasicParsing -Uri "$script:ApiBase/metrics" -TimeoutSec 10).Content
    Assert-True ($metrics -match "observatory_process_ready 1") "normalizer metrics did not expose readiness"
    $result.checks.health = @{ status = "pass"; api = $health.status }

    $ingest = Invoke-ObservatoryCli @("ingest", "--file", $fixtureFile, "--url", "$script:ApiBase/v1/events")
    Assert-True ($ingest.data.inserted -gt 0) "synthetic JSONL was not delivered to the normalizer"
    $result.checks.synthetic_ingest = @{ status = "pass"; inserted = $ingest.data.inserted }
    $fixtureSummary = (Get-JsonEndpoint "$script:ApiBase/v1/summary").data
    Assert-True ($fixtureSummary.tool_calls -ge 5 -and $fixtureSummary.files_changed -ge 2 -and $fixtureSummary.commands_executed -ge 5 -and $fixtureSummary.tests_invoked -ge 3) "normalized agent behavior counts were not aggregated"
    Assert-True ($fixtureSummary.agent_failures -ge 1 -and $fixtureSummary.reassessments -ge 2 -and $fixtureSummary.rework_loops -ge 1) "normalized agent reliability dimensions were not aggregated"
    $result.checks.synthetic_agent_behavior = @{ status = "pass"; tool_calls = $fixtureSummary.tool_calls; files_changed = $fixtureSummary.files_changed; commands_executed = $fixtureSummary.commands_executed; tests_invoked = $fixtureSummary.tests_invoked; agent_failures = $fixtureSummary.agent_failures; reassessments = $fixtureSummary.reassessments; rework_loops = $fixtureSummary.rework_loops }

    $before = (Get-JsonEndpoint "$script:ApiBase/v1/summary").data.events
    $otlp = Send-SyntheticOtlpTrace
    Assert-True ($otlp.StatusCode -ge 200 -and $otlp.StatusCode -lt 300) "Collector OTLP request was not accepted"
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        $after = (Get-JsonEndpoint "$script:ApiBase/v1/summary").data.events
        if ($after -gt $before) { break }
        Start-Sleep -Seconds 1
    } while ([DateTime]::UtcNow -lt $deadline)
    Assert-True ($after -gt $before) "Collector-to-normalizer trace delivery was not observed"
    $result.checks.collector_delivery = @{ status = "pass"; response = $otlp.StatusCode; events_before = $before; events_after = $after }

    $claudeBefore = (Get-JsonEndpoint "$script:ApiBase/v1/summary").data.events
    $claudeOtlp = Send-ClaudeShapedOtlpLog
    Assert-True ($claudeOtlp.StatusCode -ge 200 -and $claudeOtlp.StatusCode -lt 300) "Claude-shaped plain-key OTLP log was not accepted"
    $claudeDeadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    $claudeEvent = $null
    do {
        $claudeCandidates = @(Get-Events | Where-Object {
            ([string]$_.event_type -eq "model.operation") -and
                ([string]$_.source.name -eq "claude-code") -and
                ([string]$_.llm.provider -eq "anthropic") -and
                ([string]$_.llm.model -eq "runtime-claude-model") -and
                ([string]$_.execution.session_id -eq "runtime-claude-session")
        })
        if ($claudeCandidates.Count -gt 0) {
            $claudeEvent = $claudeCandidates[-1]
            break
        }
        Start-Sleep -Seconds 1
    } while ([DateTime]::UtcNow -lt $claudeDeadline)
    Assert-True ($null -ne $claudeEvent) "Claude-shaped plain-key OTLP log did not normalize to an attributable model operation"
    Assert-True ([double]$claudeEvent.usage.input_tokens -eq 11 -and [double]$claudeEvent.usage.output_tokens -eq 7 -and [double]$claudeEvent.performance.duration_ms -eq 19 -and [double]$claudeEvent.performance.time_to_first_token_ms -eq 5) "Claude-shaped plain-key OTLP measurements were not normalized"
    $result.checks.claude_plain_key_mapping = @{ status = "pass"; events_before = $claudeBefore; event_type = $claudeEvent.event_type; provider = $claudeEvent.llm.provider; model = $claudeEvent.llm.model; session_id = $claudeEvent.execution.session_id; client = $claudeEvent.llm.client }

    $projectTraceId = "abcdefabcdefabcdefabcdefabcdefab"
    $projectSpanId = "abcdefabcdefabcd"
    $projectOtlp = Send-SyntheticOtlpTrace -TraceId $projectTraceId -SpanId $projectSpanId -ProjectRootOnly
    Assert-True ($projectOtlp.StatusCode -ge 200 -and $projectOtlp.StatusCode -lt 300) "Collector project-attribution trace was not accepted"
    $projectDeadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    $projectEvent = $null
    do {
        try {
            $projectEvent = Get-JsonEndpoint "$script:ApiBase/v1/events/otel:$projectTraceId`:$projectSpanId"
            if ($projectEvent.event.project.project_id -like "local_sha256:*") { break }
        } catch { }
        Start-Sleep -Seconds 1
    } while ([DateTime]::UtcNow -lt $projectDeadline)
    Assert-True ($null -ne $projectEvent -and $projectEvent.event.project.project_id -like "local_sha256:*" -and $null -eq $projectEvent.event.project.root) "Collector did not derive a safe project identity before deleting the raw project root"
    $result.checks.project_attribution = @{ status = "pass"; project_id = $projectEvent.event.project.project_id; raw_root_persisted = $false }

    $cwdTraceId = "1234567890abcdef1234567890abcdef"
    $cwdSpanId = "1234567890abcdef"
    $cwdOtlp = Send-SyntheticOtlpTrace -TraceId $cwdTraceId -SpanId $cwdSpanId -ProjectWorkingDirectoryOnly
    Assert-True ($cwdOtlp.StatusCode -ge 200 -and $cwdOtlp.StatusCode -lt 300) "Collector working-directory project-attribution trace was not accepted"
    $cwdDeadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    $cwdEvent = $null
    do {
        try {
            $cwdEvent = Get-JsonEndpoint "$script:ApiBase/v1/events/otel:$cwdTraceId`:$cwdSpanId"
            if ($cwdEvent.event.project.project_id -like "local_sha256:*") { break }
        } catch { }
        Start-Sleep -Seconds 1
    } while ([DateTime]::UtcNow -lt $cwdDeadline)
    Assert-True ($null -ne $cwdEvent -and $cwdEvent.event.project.project_id -like "local_sha256:*" -and $null -eq $cwdEvent.event.project.root) "Collector did not derive a safe project identity from a native working-directory attribute"
    $result.checks.project_attribution_working_directory = @{ status = "pass"; project_id = $cwdEvent.event.project.project_id; raw_working_directory_persisted = $false }

    $privacyBefore = (Get-JsonEndpoint "$script:ApiBase/v1/summary").data.events
    $privacyOtlp = Send-SyntheticOtlpTrace -TraceId "fedcba9876543210fedcba9876543210" -SpanId "fedcba9876543210" -IncludePrivacyCanary
    $privacyLog = Send-SyntheticOtlpLog -IncludePrivacyCanary
    $privacyMetric = Send-SyntheticOtlpMetric -IncludePrivacyCanary
    Assert-True ($privacyOtlp.StatusCode -ge 200 -and $privacyOtlp.StatusCode -lt 300) "Collector privacy canary was not accepted"
    Assert-True ($privacyLog.StatusCode -ge 200 -and $privacyLog.StatusCode -lt 300) "Collector privacy log canary was not accepted"
    Assert-True ($privacyMetric.StatusCode -ge 200 -and $privacyMetric.StatusCode -lt 300) "Collector privacy metric canary was not accepted"
    $privacyDeadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        $privacyAfter = (Get-JsonEndpoint "$script:ApiBase/v1/summary").data.events
        if ($privacyAfter -ge ($privacyBefore + 3)) { break }
        Start-Sleep -Seconds 1
    } while ([DateTime]::UtcNow -lt $privacyDeadline)
    Assert-True ($privacyAfter -ge ($privacyBefore + 3)) "Collector privacy trace/log/metric canaries were not delivered to the normalizer"
    $eventJson = (Get-JsonEndpoint "$script:ApiBase/v1/events?limit=100" | ConvertTo-Json -Depth 40)
    $privacyCanaries = @("RUNTIME_PROMPT_CANARY", "RUNTIME_COMPLETION_CANARY", "RUNTIME_API_KEY_CANARY", "RUNTIME_SECRET_CANARY", "RUNTIME_BODY_CANARY", "RUNTIME_AUTH_TOKEN_CANARY", "RUNTIME_CLIENT_SECRET_CANARY", "RUNTIME_PROMPT_TEXT_CANARY", "RUNTIME_TOOL_ARGUMENTS_CANARY", "RUNTIME_UNKNOWN_SENSITIVE_CANARY", "RUNTIME_UNKNOWN_ATTRIBUTE_CANARY", "RUNTIME_PROJECT_ROOT_CANARY", "RUNTIME_PROJECT_REMOTE_CANARY", "RUNTIME_PROCESS_ENVIRONMENT_CANARY", "RUNTIME_USER_EMAIL_CANARY", "RUNTIME_ENVIRONMENT_CANARY", "RUNTIME_LOG_PROMPT_CANARY", "RUNTIME_LOG_PROMPT_ATTRIBUTE_CANARY", "RUNTIME_LOG_API_KEY_CANARY", "RUNTIME_LOG_PROCESS_ENVIRONMENT_CANARY", "RUNTIME_LOG_USER_EMAIL_CANARY", "RUNTIME_METRIC_API_KEY_CANARY", "RUNTIME_METRIC_PROCESS_ENVIRONMENT_CANARY", "RUNTIME_METRIC_USER_EMAIL_CANARY")
    foreach ($canary in $privacyCanaries) {
        Assert-True (-not $eventJson.Contains($canary)) "Collector privacy canary '$canary' was persisted"
    }

    $headers = Wait-GrafanaApi
    $datasources = Invoke-RestMethod -Headers $headers -Uri "$script:GrafanaBase/api/datasources" -TimeoutSec 10
    $sourceNames = @($datasources | ForEach-Object { $_.name })
    foreach ($requiredSource in @("Observatory Events", "Prometheus", "Tempo", "Loki")) {
        Assert-True ($sourceNames -contains $requiredSource) "Grafana datasource '$requiredSource' was not provisioned"
    }
    $dashboardUids = Get-ChildItem -LiteralPath (Join-Path $repositoryRoot "dashboards") -Filter "*.json" | ForEach-Object {
        (Get-Content -LiteralPath $_.FullName -Raw | ConvertFrom-Json).uid
    }
    foreach ($uid in $dashboardUids) {
        $dashboard = Invoke-RestMethod -Headers $headers -Uri "$script:GrafanaBase/api/dashboards/uid/$uid" -TimeoutSec 10
        Assert-True ($null -eq $dashboard.meta.isDisabled -or $dashboard.meta.isDisabled -eq $false) "Grafana dashboard '$uid' is disabled"
    }
    $result.checks.grafana = @{ status = "pass"; datasources = $sourceNames; dashboards = @($dashboardUids) }
    $grafanaEventCount = Wait-GrafanaPrometheusMetric -Headers $headers
    Assert-True ($grafanaEventCount -ge $after) "Grafana Observatory Events query did not expose the normalized event count"
    $result.checks.grafana_metrics = @{ status = "pass"; observed_events = $grafanaEventCount }
    $eventTimeRange = Invoke-GrafanaEventTimeRangeProbe -Headers $headers
    $result.checks.event_time_query_range = $eventTimeRange
    $collectorSelfMetric = Wait-GrafanaCollectorSelfMetric -Headers $headers
    $result.checks.collector_self_observability = @{ status = "pass"; metric = "otelcol_process_uptime"; samples = @($collectorSelfMetric.data.result).Count }
    $result.checks.session_trace_query = Invoke-GrafanaTempoSessionProbe -Headers $headers

    $tempoPrivacySearchJson = ""
    $tempoPrivacyDeadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        try {
            $tempoPrivacySearch = Invoke-RestMethod -Headers $headers -Uri "$script:GrafanaBase/api/datasources/proxy/uid/tempo/api/search?limit=100" -TimeoutSec 10
            $tempoPrivacySearchJson = $tempoPrivacySearch | ConvertTo-Json -Depth 40
            if ($tempoPrivacySearchJson.Contains("fedcba9876543210fedcba9876543210")) { break }
        } catch { }
        Start-Sleep -Seconds 1
    } while ([DateTime]::UtcNow -lt $tempoPrivacyDeadline)
    Assert-True ($tempoPrivacySearchJson.Contains("fedcba9876543210fedcba9876543210")) "Tempo privacy trace was not indexed"
    $tempoPrivacy = Invoke-RestMethod -Headers $headers -Uri "$script:GrafanaBase/api/datasources/proxy/uid/tempo/api/traces/fedcba9876543210fedcba9876543210" -TimeoutSec 10
    $tempoPrivacyJson = $tempoPrivacy | ConvertTo-Json -Depth 40
    Assert-True ($tempoPrivacyJson.Length -gt 0) "Tempo privacy trace detail was empty"
    foreach ($canary in $privacyCanaries) {
        Assert-True (-not $tempoPrivacyJson.Contains($canary)) "Tempo persisted privacy canary '$canary'"
    }

    $lokiPrivacy = Invoke-RestMethod -Headers $headers -Uri "$script:GrafanaBase/api/datasources/proxy/uid/loki/loki/api/v1/query_range?query=$([Uri]::EscapeDataString('{service_name="runtime-acceptance"}'))&limit=100&direction=backward" -TimeoutSec 10
    $lokiPrivacyJson = $lokiPrivacy | ConvertTo-Json -Depth 40
    Assert-True (@($lokiPrivacy.data.result).Count -gt 0) "Loki privacy log was not queryable"
    foreach ($canary in $privacyCanaries) {
        Assert-True (-not $lokiPrivacyJson.Contains($canary)) "Loki persisted privacy canary '$canary'"
    }

    $prometheusPrivacyQuery = [Uri]::EscapeDataString("runtime_privacy_metric")
    $prometheusPrivacy = Invoke-RestMethod -Headers $headers -Uri "$script:GrafanaBase/api/datasources/proxy/uid/system-prometheus/api/v1/query?query=$prometheusPrivacyQuery" -TimeoutSec 10
    $prometheusPrivacyJson = $prometheusPrivacy | ConvertTo-Json -Depth 40
    Assert-True (@($prometheusPrivacy.data.result).Count -gt 0) "Prometheus privacy metric was not queryable"
    foreach ($canary in $privacyCanaries) {
        Assert-True (-not $prometheusPrivacyJson.Contains($canary)) "Prometheus persisted privacy canary '$canary'"
    }
    $result.checks.collector_privacy_boundary = @{ status = "pass"; events_before = $privacyBefore; events_after = $privacyAfter; canaries = "redacted"; stores = @("normalizer", "tempo", "loki", "prometheus") }

    $malformedStatus = 0
    try {
        Invoke-WebRequest -UseBasicParsing -Method Post -Uri "$script:ApiBase/v1/events" -ContentType "application/json" -Body '{"schema_version":"1.0"}' -TimeoutSec 10 | Out-Null
    } catch {
        if ($null -ne $_.Exception.Response) {
            $malformedStatus = [int]$_.Exception.Response.StatusCode
        }
    }
    Assert-Equal $malformedStatus 400 "malformed telemetry was not rejected with HTTP 400"
    Wait-Http "$script:ApiBase/readyz" | Out-Null
    $result.checks.malformed_telemetry = @{ status = "pass"; http_status = $malformedStatus; inference_path = "unmanaged/no-proxy" }

    $dashboardQueryProbe = Invoke-GrafanaDashboardQueryProbe -Headers $headers -DashboardUids @($dashboardUids)
    Assert-True ($dashboardQueryProbe.Count -gt 0) "Grafana dashboard query probe did not execute any panel queries"
    $result.checks.dashboard_query_probe = @{ status = "pass"; dashboards = @($dashboardQueryProbe.dashboard | Select-Object -Unique); queries = $dashboardQueryProbe.Count; signals = @($dashboardQueryProbe.signal | Group-Object | ForEach-Object { "$($_.Name):$($_.Count)" }) }
    $dashboardFilterProbe = Invoke-GrafanaDashboardFilterProbe -Headers $headers
    $result.checks.dashboard_filter_probe = @{ status = "pass"; queries = @($dashboardFilterProbe.Keys); project_alpha_events = $dashboardFilterProbe.project_alpha_events; project_beta_events = $dashboardFilterProbe.project_beta_events }

    Invoke-Compose @("restart", "observatory-api", "otel-collector", "grafana") | Out-Null
    Wait-Http "$script:ApiBase/readyz" | Out-Null
    Wait-Http "$script:CollectorBase/" | Out-Null
    Wait-Http "$script:GrafanaBase/api/health" | Out-Null
    Wait-GrafanaApi | Out-Null
    Wait-GrafanaPrometheusMetric -Headers (Get-GrafanaHeaders) | Out-Null
    $result.checks.restart_recovery = @{ status = "pass" }

    Invoke-Compose @("stop", "grafana") | Out-Null
    Wait-Http "$script:ApiBase/readyz" | Out-Null
    $grafanaSentinel = Invoke-InferenceSentinel
    $result.checks.grafana_failure_isolation = @{ status = "pass"; api = "ready"; inference_sentinel = $grafanaSentinel.output }
    Invoke-Compose @("start", "grafana") | Out-Null
    Wait-Http "$script:GrafanaBase/api/health" | Out-Null
    Wait-GrafanaApi | Out-Null

    Invoke-Compose @("stop", "otel-collector") | Out-Null
    Wait-Http "$script:ApiBase/readyz" | Out-Null
    $collectorSentinel = Invoke-InferenceSentinel
    $result.checks.collector_failure_isolation = @{ status = "pass"; api = "ready"; inference_sentinel = $collectorSentinel.output }
    Invoke-Compose @("start", "otel-collector") | Out-Null
    Wait-Http "$script:CollectorBase/" | Out-Null

    $queuedBefore = (Get-JsonEndpoint "$script:ApiBase/v1/summary").data.events
    $queuedTraceId = "00112233445566770011223344556677"
    $queuedSpanId = "0011223344556677"
    $queuedEventId = "otel:$queuedTraceId`:$queuedSpanId"
    Invoke-Compose @("stop", "observatory-api") | Out-Null
    $queuedOtlp = Send-SyntheticOtlpTrace -TraceId $queuedTraceId -SpanId $queuedSpanId
    Assert-True ($queuedOtlp.StatusCode -ge 200 -and $queuedOtlp.StatusCode -lt 300) "Collector did not accept telemetry while the normalizer was unavailable"
    Invoke-Compose @("start", "observatory-api") | Out-Null
    Wait-Http "$script:ApiBase/readyz" | Out-Null
    $queueDeadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        $queuedAfter = (Get-JsonEndpoint "$script:ApiBase/v1/summary").data.events
        if ($queuedAfter -gt $queuedBefore) { break }
        Start-Sleep -Seconds 1
    } while ([DateTime]::UtcNow -lt $queueDeadline)
    Assert-True ($queuedAfter -gt $queuedBefore) "Collector queued telemetry was not delivered after normalizer recovery"
    $queuedEvent = Get-JsonEndpoint "$script:ApiBase/v1/events/$queuedEventId"
    Assert-True ($queuedEvent.event.event_id -eq $queuedEventId) "Collector queued telemetry did not retain the expected event identity after normalizer recovery"
    $result.checks.normalizer_outage_queue_recovery = @{ status = "pass"; events_before = $queuedBefore; events_after = $queuedAfter; collector_response = $queuedOtlp.StatusCode; event_id = $queuedEventId }

    $recoveryArchive = Join-Path ([IO.Path]::GetTempPath()) "llm-observatory-acceptance-full-state-$([Guid]::NewGuid().ToString('N')).zip"
    $recoveryEventsBefore = (Get-JsonEndpoint "$script:ApiBase/v1/summary").data.events
    $recoveryEventIdsBefore = @{}
    foreach ($event in (Get-Events)) {
        if ($null -ne $event.event_id) { $recoveryEventIdsBefore[[string]$event.event_id] = $true }
    }
    $recoveryLog = Send-SyntheticOtlpLog
    Assert-True ($recoveryLog.StatusCode -ge 200 -and $recoveryLog.StatusCode -lt 300) "Collector did not accept the recovery log fixture"
    $recoveryLogDeadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    $recoveryEvent = $null
    do {
        $recoveryCandidates = @(Get-Events | Where-Object {
            $eventId = [string]$_.event_id
            $eventType = [string]$_.event_type
            $sourceName = [string]$_.source.name
            $eventId -and -not $recoveryEventIdsBefore.ContainsKey($eventId) -and $eventType -eq "telemetry.log" -and $sourceName -eq "runtime-acceptance"
        })
        if ($recoveryCandidates.Count -gt 0) {
            $recoveryEvent = $recoveryCandidates[-1]
            break
        }
        Start-Sleep -Seconds 1
    } while ([DateTime]::UtcNow -lt $recoveryLogDeadline)
    Assert-True ($null -ne $recoveryEvent) "Recovery log fixture was not normalized before full-state backup"
    $recoveryEventId = [string]$recoveryEvent.event_id
    Invoke-Compose @("stop") | Out-Null
    $backup = Invoke-ObservatoryCli @("backup", $recoveryArchive, "--full-state", "--backend-volumes", "--include-secret", "--compose-file", $composeFile, "--overwrite")
    Assert-Equal $backup.outcome "success" "full-state backend-volume backup did not succeed"
    Assert-Equal $backup.data.docker_named_volumes "included" "full-state backup did not include backend volumes"
    Invoke-Compose @("down", "--remove-orphans", "--volumes") | Out-Null
    $restore = Invoke-ObservatoryCli @("restore", $recoveryArchive, "--full-state", "--backend-volumes", "--restore-secret", "--compose-file", $composeFile, "--api-health-url", "$script:ApiBase/healthz", "--overwrite")
    Assert-Equal $restore.outcome "success" "full-state backend-volume restore did not succeed"
    $recoveredStart = Invoke-ObservatoryCli @("start", "--compose-file", $composeFile, "--api-url", "$script:ApiBase/readyz", "--collector-url", "$script:CollectorBase/", "--grafana-url", "$script:GrafanaBase/api/health")
    Assert-Equal $recoveredStart.outcome "success" "stack did not restart after full-state restore"
    $recoveredSummary = (Get-JsonEndpoint "$script:ApiBase/v1/summary").data.events
    Assert-True ($recoveredSummary -ge $recoveryEventsBefore) "normalized events were not retained across full-state restore"
    $recoveredMetric = Wait-GrafanaPrometheusMetric -Headers (Wait-GrafanaApi)
    Assert-True ($recoveredMetric -ge $recoveryEventsBefore) "Prometheus data was not retained across full-state restore"
    $recoveryTempoQuery = [Uri]::EscapeDataString(('{ trace:id = "' + $queuedTraceId + '" }'))
    $tempoSearch = $null
    $recoveryTraceDeadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        try {
            $tempoSearch = Invoke-RestMethod -Headers (Get-GrafanaHeaders) -Uri "$script:GrafanaBase/api/datasources/proxy/uid/tempo/api/search?q=$recoveryTempoQuery&limit=20" -TimeoutSec 10
            if (@($tempoSearch.traces).Count -gt 0) { break }
        } catch { }
        Start-Sleep -Seconds 1
    } while ([DateTime]::UtcNow -lt $recoveryTraceDeadline)
    Assert-True ($null -ne $tempoSearch -and @($tempoSearch.traces).Count -gt 0) "The known recovery trace was not retained across full-state restore"
    $recoveredLogEvent = Get-JsonEndpoint "$script:ApiBase/v1/events/$recoveryEventId"
    Assert-True ($recoveredLogEvent.event.event_id -eq $recoveryEventId -and $recoveredLogEvent.event.event_type -eq "telemetry.log") "The known recovery log event was not retained across full-state restore"
    $lokiLabels = Invoke-RestMethod -Headers (Get-GrafanaHeaders) -Uri "$script:GrafanaBase/api/datasources/proxy/uid/loki/loki/api/v1/labels" -TimeoutSec 10
    Assert-True (@($lokiLabels.data).Count -gt 0) "Loki labels were not retained across full-state restore"
    $result.checks.full_disaster_recovery = @{ status = "pass"; archive = $recoveryArchive; events_before = $recoveryEventsBefore; events_after = $recoveredSummary; prometheus_events = $recoveredMetric; recovery_trace_event_id = $queuedEventId; recovery_log_event_id = $recoveryEventId; tempo_traces = @($tempoSearch.traces).Count; recovered_log_event = $recoveredLogEvent.event.event_id; loki_labels = @($lokiLabels.data).Count }

    Invoke-Compose @("stop", "observatory-api") | Out-Null
    $storageSentinel = Invoke-InferenceSentinel
    $plan = Invoke-ObservatoryCli -CliArgs @("configure", "all") -AllowedExitCodes @(0, 5)
    Assert-True ($plan.outcome -in @("partial", "success")) "client configuration plan depended on the normalizer"
    $result.checks.storage_failure_isolation = @{ status = "pass"; normalizer_storage = "unavailable"; client_plan = $plan.outcome; inference_sentinel = $storageSentinel.output; inference_proxy = $plan.data.inference_proxy }
    $result.checks.api_failure_isolation = @{ status = "pass"; client_plan = $plan.outcome; inference_sentinel = $storageSentinel.output; inference_proxy = $plan.data.inference_proxy }
    Invoke-Compose @("start", "observatory-api") | Out-Null
    Wait-Http "$script:ApiBase/readyz" | Out-Null

    $status = Invoke-ObservatoryCli @("status", "--url", "$script:ApiBase/healthz", "--grafana-url", "$script:GrafanaBase/api/health", "--collector-url", "$script:CollectorBase/")
    Assert-Equal $status.outcome "success" "final status did not report a ready Observatory"
    $result.checks.final_status = @{ status = "pass"; observatory = $status.data.observatory; inference_path = $status.data.inference_path }
} catch {
    $result.status = "fail"
    $result.error = $_.Exception.Message
    $result.error_location = $_.InvocationInfo.PositionMessage
    $result.error_stack = $_.ScriptStackTrace
} finally {
    try {
        if ($null -ne $recoveryArchive -and (Test-Path -LiteralPath $recoveryArchive)) {
            Remove-Item -LiteralPath $recoveryArchive -Force
        }
        if (Test-Path -LiteralPath (Join-Path $statePath "compose.env")) {
            $downArgs = @("down", "--remove-orphans")
            if (-not $KeepVolumes) {
                $downArgs += "--volumes"
            }
            Invoke-Compose $downArgs | Out-Null
        }
    } catch {
        $result.status = "fail"
        $result.cleanup_warning = $_.Exception.Message
    }
    if ($temporaryState -and -not $KeepState -and (Test-Path -LiteralPath $statePath)) {
        Remove-Item -LiteralPath $statePath -Recurse -Force
    }
    try {
        $apiImageIds = @(& docker image ls --quiet $acceptanceApiImage 2>$null)
        if ($LASTEXITCODE -eq 0 -and $apiImageIds.Count -gt 0) {
            $removedApiImage = & docker image rm $acceptanceApiImage 2>&1
            if ($LASTEXITCODE -ne 0) {
                $result.status = "fail"
                $result.cleanup_warning = "acceptance API image cleanup failed: $(($removedApiImage | Out-String).Trim())"
            }
        }
    } catch {
        $result.status = "fail"
        $result.cleanup_warning = "acceptance API image cleanup failed: $($_.Exception.Message)"
    }
    if ($null -eq $previousProjectName) { Remove-Item Env:COMPOSE_PROJECT_NAME -ErrorAction SilentlyContinue } else { $env:COMPOSE_PROJECT_NAME = $previousProjectName }
    if ($null -eq $previousPythonPath) { Remove-Item Env:PYTHONPATH -ErrorAction SilentlyContinue } else { $env:PYTHONPATH = $previousPythonPath }
    if ($null -eq $previousApiImage) { Remove-Item Env:OBSERVATORY_API_IMAGE -ErrorAction SilentlyContinue } else { $env:OBSERVATORY_API_IMAGE = $previousApiImage }
}

$result | ConvertTo-Json -Depth 40
if ($result.status -ne "pass") {
    exit 1
}
