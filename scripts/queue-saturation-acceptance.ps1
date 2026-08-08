[CmdletBinding()]
param(
    [string]$Python = "python",
    [int]$TimeoutSeconds = 60,
    [int]$Requests = 64,
    [switch]$KeepContainer
)

$ErrorActionPreference = "Stop"
$image = "otel/opentelemetry-collector-contrib@sha256:c5918f78992ee73b0d6f0e599423ac5ec52dd5d9726733114d6eca53d5a32ed5"
$containerName = "llm-observatory-queue-saturation-$([Guid]::NewGuid().ToString('N').Substring(0, 12))"
$temporaryRoot = Join-Path ([IO.Path]::GetTempPath()) "llm-observatory-queue-saturation-$([Guid]::NewGuid().ToString('N'))"
$configPath = Join-Path $temporaryRoot "collector.yaml"
$hostPort = $null
$metricsPort = $null

function Assert-True {
    param([Parameter(Mandatory = $true)][bool]$Condition, [Parameter(Mandatory = $true)][string]$Message)
    if (-not $Condition) { throw $Message }
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
    Assert-True ($exitCode -eq 0 -and $text -eq "inference-sentinel") "unrelated inference sentinel was affected"
    return [ordered]@{ exit_code = $exitCode; output = "inference-sentinel" }
}

function Send-SaturationTrace {
    param([Parameter(Mandatory = $true)][int]$Sequence)
    $hex = $Sequence.ToString("x").PadLeft(32, "0")
    $span = $Sequence.ToString("x").PadLeft(16, "0")
    $now = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() * 1000000
    $payload = @{
        resourceSpans = @(@{
            resource = @{ attributes = @(@{ key = "service.name"; value = @{ stringValue = "queue-saturation-acceptance" } }) }
            scopeSpans = @(@{
                spans = @(@{
                    traceId = $hex
                    spanId = $span
                    name = "gen_ai.queue_saturation"
                    startTimeUnixNano = "$now"
                    endTimeUnixNano = "$($now + 1000000)"
                    attributes = @(@{ key = "gen_ai.provider.name"; value = @{ stringValue = "synthetic" } })
                })
            })
        })
    } | ConvertTo-Json -Depth 20
    return Invoke-WebRequest -Method Post -Uri "http://127.0.0.1:$hostPort/v1/traces" -ContentType "application/json" -Body $payload -TimeoutSec 10
}

function Get-CollectorMetrics {
    return (Invoke-WebRequest -Method Get -Uri "http://127.0.0.1:$metricsPort/metrics" -TimeoutSec 10).Content
}

function Get-MetricSum {
    param(
        [Parameter(Mandatory = $true)][string]$Metrics,
        [Parameter(Mandatory = $true)][string]$Name
    )
    $total = 0.0
    foreach ($line in ($Metrics -split "`r?`n")) {
        if ($line -match "^$([regex]::Escape($Name))(?:\{[^}]*\})?\s+([-+0-9.eE]+)\s*$") {
            $total += [double]$Matches[1]
        }
    }
    return $total
}

$result = [ordered]@{
    schema = "observatory.queue-saturation/v1"
    status = "pass"
    image = $image
    queue_size = 2
    requests = $Requests
    container = $containerName
    checks = [ordered]@{}
}

try {
    Assert-True ($Requests -ge 8) "Requests must be at least 8 to exercise the bounded queue"
    Assert-True ($TimeoutSeconds -ge 20) "TimeoutSeconds must be at least 20"
    $hostPort = Get-FreeLoopbackPort
    $metricsPort = Get-FreeLoopbackPort
    Assert-True ($metricsPort -ne $hostPort) "OTLP and self-metrics ports must be distinct"
    New-Item -ItemType Directory -Path $temporaryRoot -Force | Out-Null
    @'
receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318
        max_request_body_size: 8388608

processors:
  batch:
    timeout: 100ms
    send_batch_size: 1
    send_batch_max_size: 1

exporters:
  otlphttp/blackhole:
    endpoint: http://127.0.0.1:9
    timeout: 1s
    sending_queue:
      enabled: true
      queue_size: 2
      block_on_overflow: false
    retry_on_failure:
      enabled: true
      initial_interval: 100ms
      max_interval: 500ms
      max_elapsed_time: 2s

service:
  telemetry:
    metrics:
      level: normal
      readers:
        - pull:
            exporter:
              prometheus:
                host: 0.0.0.0
                port: 8888
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlphttp/blackhole]
'@ | Set-Content -LiteralPath $configPath -Encoding utf8NoBOM

    $containerId = & docker run -d --pull=never --name $containerName -p "127.0.0.1:${hostPort}:4318" -p "127.0.0.1:${metricsPort}:8888" -v "${configPath}:/etc/otelcol/config.yaml:ro" $image --config=/etc/otelcol/config.yaml
    Assert-True ($LASTEXITCODE -eq 0 -and -not [string]::IsNullOrWhiteSpace(($containerId | Out-String))) "queue-saturation Collector did not start"
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    $running = $false
    do {
        $state = (& docker inspect --format '{{.State.Status}}' $containerName 2>$null | Out-String).Trim()
        if ($state -eq "running") { $running = $true; break }
        if ($state -eq "exited" -or $state -eq "dead") { break }
        Start-Sleep -Milliseconds 500
    } while ([DateTime]::UtcNow -lt $deadline)
    Assert-True $running "queue-saturation Collector did not remain running"
    $result.checks.collector = @{ status = "pass"; queue_size = 2; self_metrics_endpoint = "http://127.0.0.1:$metricsPort/metrics" }

    $receiverReady = $false
    $receiverDeadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        try {
            $probe = Send-SaturationTrace -Sequence 0
            if ($probe.StatusCode -ge 200 -and $probe.StatusCode -lt 300) { $receiverReady = $true; break }
        } catch {
            # The container can be running while the Collector is still loading its configuration.
        }
        Start-Sleep -Milliseconds 500
    } while ([DateTime]::UtcNow -lt $receiverDeadline)
    Assert-True $receiverReady "queue-saturation Collector OTLP receiver did not become ready"
    $result.checks.receiver = @{ status = "pass"; endpoint = "http://127.0.0.1:$hostPort/v1/traces" }

    $accepted = 1
    for ($sequence = 1; $sequence -le $Requests; $sequence++) {
        try {
            $response = Send-SaturationTrace -Sequence $sequence
            if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 300) { $accepted++ }
        } catch {
            # Receiver-side rejection is allowed once the bounded exporter path is saturated.
        }
    }
    Assert-True ($accepted -gt 0) "queue-saturation Collector accepted no telemetry"
    $sentinel = Invoke-InferenceSentinel
    $result.checks.inference_isolation = $sentinel

    $metrics = ""
    $metricsReady = $false
    $metricsDeadline = [DateTime]::UtcNow.AddSeconds([Math]::Min($TimeoutSeconds, 30))
    do {
        try {
            $metrics = Get-CollectorMetrics
            if ($metrics -match "(?m)^otelcol_exporter_queue_capacity(?:\{[^}]*\})?\s+") {
                $metricsReady = $true
                break
            }
        } catch {
            # The Collector can be running before its internal Prometheus reader is ready.
        }
        Start-Sleep -Milliseconds 500
    } while ([DateTime]::UtcNow -lt $metricsDeadline)
    Assert-True $metricsReady "Collector self-metrics endpoint did not expose exporter queue capacity"

    $queueCapacity = Get-MetricSum -Metrics $metrics -Name "otelcol_exporter_queue_capacity"
    $queueSize = Get-MetricSum -Metrics $metrics -Name "otelcol_exporter_queue_size"
    $enqueueFailedSpans = Get-MetricSum -Metrics $metrics -Name "otelcol_exporter_enqueue_failed_spans"
    $sendFailedSpans = Get-MetricSum -Metrics $metrics -Name "otelcol_exporter_send_failed_spans"
    Assert-True ($queueCapacity -ge 2) "Collector self-metrics did not report the configured bounded queue capacity"
    Assert-True ($queueSize -ge 2) "Collector self-metrics did not show the bounded queue filling under load"
    Assert-True (($enqueueFailedSpans + $sendFailedSpans) -gt 0) "Collector self-metrics did not report an enqueue or send failure under saturation"
    $result.checks.self_metrics = @{
        status = "pass"
        queue_capacity = $queueCapacity
        queue_size = $queueSize
        enqueue_failed_spans = $enqueueFailedSpans
        send_failed_spans = $sendFailedSpans
    }

    $evidencePattern = "(?is)(?:sending_queue|queue).{0,120}(?:full|drop|reject)|(?:drop|dropped|dropping).{0,120}(?:data|span|request)|(?:failed to send|exporting failed)"
    $logs = ""
    $failureEvidence = $null
    $logDeadline = [DateTime]::UtcNow.AddSeconds([Math]::Min($TimeoutSeconds, 30))
    do {
        $logs = (& docker logs $containerName 2>&1 | Out-String)
        $match = [regex]::Match($logs, $evidencePattern)
        if ($match.Success) {
            $failureEvidence = $match.Value.Trim()
            break
        }
        Start-Sleep -Milliseconds 500
    } while ([DateTime]::UtcNow -lt $logDeadline)
    Assert-True (-not [string]::IsNullOrWhiteSpace($failureEvidence)) "Collector logs did not expose bounded exporter failure/drop evidence"
    $result.checks.exporter_queue = @{ status = "pass"; accepted_requests = $accepted; attempted_requests = $Requests; rejected_requests = ($Requests + 1 - $accepted); failure_evidence = $failureEvidence }
} catch {
    $result.status = "fail"
    $result.error = $_.Exception.Message
    $result.error_location = $_.InvocationInfo.PositionMessage
} finally {
    if (-not $KeepContainer) {
        & docker rm -f $containerName 2>$null | Out-Null
    }
    if (Test-Path -LiteralPath $temporaryRoot) {
        Remove-Item -LiteralPath $temporaryRoot -Recurse -Force
    }
}

$result | ConvertTo-Json -Depth 20
if ($result.status -ne "pass") { exit 1 }
