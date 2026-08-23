<#
.SYNOPSIS
    Starts the Kafka stack and waits until it is genuinely ready to serve.
.DESCRIPTION
    Brings up Kafka, the Schema Registry, the topic-creation job and Kafka UI, then blocks
    until the broker and registry pass their healthchecks. Returning only once the stack is
    actually usable means the producer and consumer never race a half-started broker.
#>
[CmdletBinding()]
param(
    [int]$TimeoutSeconds = 180
)

. (Join-Path $PSScriptRoot '_common.ps1')

Write-Host ""
Write-Host "  Starting the Kafka stack" -ForegroundColor Cyan
Write-Host "  ------------------------------------------------------------"

Assert-Docker

docker compose -f $script:ComposeFile up -d
if ($LASTEXITCODE -ne 0) { throw "docker compose up failed with exit code $LASTEXITCODE." }

Write-Host ""
Write-Host "  Waiting for the broker and schema registry..." -ForegroundColor DarkGray

$deadline = (Get-Date).AddSeconds($TimeoutSeconds)
$ready = $false

while ((Get-Date) -lt $deadline) {
    $kafkaState = (docker inspect --format '{{.State.Health.Status}}' kafka 2>$null)
    $registryState = (docker inspect --format '{{.State.Health.Status}}' schema-registry 2>$null)

    if ($kafkaState -eq 'healthy' -and $registryState -eq 'healthy') {
        $ready = $true
        break
    }
    Start-Sleep -Seconds 3
}

if (-not $ready) {
    Write-Host ""
    Write-Host "  The stack did not become healthy within $TimeoutSeconds seconds." -ForegroundColor Red
    Write-Host "  Check the logs with:" -ForegroundColor Yellow
    Write-Host "      docker compose -f docker/docker-compose.yml logs kafka schema-registry"
    throw "Kafka stack failed to start."
}

# A healthy broker only means it is accepting connections. It says nothing about whether the
# topics this application needs actually exist: kafka-init is a separate one-shot job that can
# fail on its own, and auto-creation is deliberately disabled in the compose file. Without this
# check the script happily reports "Ready" for a stack the producer cannot use -- a false green
# in the one script the live demo depends on, which is worse than an honest failure.
$requiredTopics = @('orders', 'orders.DLQ')
$existingTopics = @(
    (docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>$null) -split "`n" |
        ForEach-Object { $_.Trim() } |
        Where-Object { $_ -ne '' }
)
$missingTopics = @($requiredTopics | Where-Object { $existingTopics -notcontains $_ })

if ($missingTopics.Count -gt 0) {
    Write-Host ""
    Write-Host "  The broker is healthy, but these required topics are missing:" -ForegroundColor Red
    Write-Host "      $($missingTopics -join ', ')" -ForegroundColor Red
    Write-Host ""
    Write-Host "  Topic creation runs in the kafka-init container. Its last output was:" -ForegroundColor Yellow
    docker logs kafka-init 2>&1 | Select-Object -Last 20
    Write-Host ""
    Write-Host "  Retry topic creation with:" -ForegroundColor Yellow
    Write-Host "      docker compose -f docker/docker-compose.yml up -d --force-recreate kafka-init"
    throw "Topic creation failed; the stack is not usable."
}

Write-Host ""
Write-Host "  Topics:" -ForegroundColor DarkGray
foreach ($topic in $existingTopics) {
    if ($requiredTopics -contains $topic) {
        Write-Host "      $topic" -ForegroundColor Green
    } else {
        Write-Host "      $topic" -ForegroundColor DarkGray
    }
}

Write-Host ""
Write-Host "  Registered schema subjects:" -ForegroundColor DarkGray
$subjects = docker exec schema-registry curl -s http://localhost:8081/subjects
if ([string]::IsNullOrWhiteSpace($subjects) -or $subjects -eq '[]') {
    Write-Host "      (none yet - the producer registers order.avsc on its first send)"
} else {
    Write-Host "      $subjects"
}

Write-Host ""
Write-Host "  ------------------------------------------------------------"
Write-Host "  Ready." -ForegroundColor Green
Write-Host "      Kafka broker    localhost:9092"
Write-Host "      Schema Registry http://localhost:8081"
Write-Host "      Kafka UI        http://localhost:8080"
Write-Host "  ------------------------------------------------------------"
Write-Host ""
