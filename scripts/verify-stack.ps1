<#
.SYNOPSIS
    Confirms the Kafka stack is genuinely usable, not merely running.

.DESCRIPTION
    A healthy broker only means it is accepting connections. It says nothing about whether the
    topics this application needs actually exist: kafka-init is a separate one-shot job that can
    fail on its own, and auto-creation is deliberately disabled in the compose file. Without this
    check the stack reports ready while the producer cannot use it - a false green in the one
    step the live demonstration depends on, which is worse than an honest failure.

.EXAMPLE
    .\scripts\verify-stack.ps1
#>

$ErrorActionPreference = 'Stop'

$requiredTopics    = @('orders.v1', 'orders.v1-retry-0', 'orders.v1-retry-1', 'orders.v1-dlt')
$schemaRegistryUrl = 'http://localhost:8081'

Write-Host "Verifying the Kafka stack..."
Write-Host ""

# --- 1. Broker reachable -----------------------------------------------------
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "  The broker is not answering on localhost:9092." -ForegroundColor Red
    Write-Host "  Start the stack with:  docker compose up -d" -ForegroundColor Yellow
    throw "Kafka broker unreachable."
}
Write-Host "  Broker           OK" -ForegroundColor Green

# --- 2. Wait for kafka-init, if it is still working --------------------------
# `docker compose up -d` returns as soon as the container starts, not when this one-shot job
# finishes. Checking the topic list straight away would report a false failure while topics
# are still being created.
$initRunning = (docker inspect -f '{{.State.Running}}' kafka-init 2>$null)
if ($initRunning -eq 'true') {
    Write-Host "  kafka-init       still running, waiting for it to finish..." -ForegroundColor DarkGray
    docker wait kafka-init | Out-Null
}

# --- 3. Every required topic exists ------------------------------------------
$existingTopics = @(
    (docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>$null) -split "`n" |
        ForEach-Object { $_.Trim() } |
        Where-Object { $_ -ne '' }
)
$missingTopics = @($requiredTopics | Where-Object { $existingTopics -notcontains $_ })

if ($missingTopics.Count -gt 0) {
    Write-Host ""
    Write-Host "  The broker is healthy, but these required topics are missing:" -ForegroundColor Red
    foreach ($topic in $missingTopics) { Write-Host "      $topic" -ForegroundColor Red }
    Write-Host ""
    Write-Host "  Topic creation runs in the kafka-init container. Its last output was:" -ForegroundColor Yellow
    docker logs kafka-init 2>&1 | Select-Object -Last 20
    Write-Host ""
    Write-Host "  Retry topic creation with:" -ForegroundColor Yellow
    Write-Host "      docker compose up -d --force-recreate kafka-init; docker wait kafka-init"
    throw "Topic creation failed; the stack is not usable."
}
Write-Host "  Topics           OK  ($($requiredTopics.Count) required topics present)" -ForegroundColor Green

# --- 4. Schema Registry responding -------------------------------------------
try {
    $subjects = Invoke-RestMethod -Uri "$schemaRegistryUrl/subjects" -TimeoutSec 10
} catch {
    Write-Host "  The Schema Registry is not answering on $schemaRegistryUrl." -ForegroundColor Red
    docker logs schema-registry 2>&1 | Select-Object -Last 20
    throw "Schema Registry unreachable."
}
$subjectList = if ($subjects.Count -gt 0) { $subjects -join ', ' } else { '(none yet)' }
Write-Host "  Schema Registry  OK  (subjects: $subjectList)" -ForegroundColor Green

Write-Host ""
Write-Host "  Topics:" -ForegroundColor DarkGray
foreach ($topic in $existingTopics) {
    if ($requiredTopics -contains $topic) {
        Write-Host "      $topic" -ForegroundColor Green
    } else {
        Write-Host "      $topic  (not required by this application)" -ForegroundColor DarkGray
    }
}

Write-Host ""
Write-Host "  Stack ready.  Kafka UI: http://localhost:8090   Schema Registry: $schemaRegistryUrl" -ForegroundColor Cyan
