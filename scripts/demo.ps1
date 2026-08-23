<#
.SYNOPSIS
    One-command live demo: brings the stack up, builds, and launches the consumer and producer
    in their own windows.
.DESCRIPTION
    Removes the fiddly part of demonstrating a streaming system, which is getting several
    processes running in the right order without fumbling between terminals. The consumer starts
    first and is given a moment to join its consumer group, so the very first orders are picked
    up rather than arriving before anyone is listening.

    Both windows stay open when the processes exit, so their final totals remain on screen.
.PARAMETER SkipBuild
    Reuse the existing jar instead of rebuilding.
.PARAMETER Rate
    Orders per second.
.EXAMPLE
    .\scripts\demo.ps1
#>
[CmdletBinding()]
param(
    [switch]$SkipBuild,
    [double]$Rate = 8
)

. (Join-Path $PSScriptRoot '_common.ps1')

Write-Host ""
Write-Host "  ============================================================" -ForegroundColor Cyan
Write-Host "   KAFKA AVRO ORDER PIPELINE - LIVE DEMO" -ForegroundColor Cyan
Write-Host "  ============================================================" -ForegroundColor Cyan

# 1. Infrastructure
Write-Host ""
Write-Host "  [1/3] Kafka stack" -ForegroundColor White
& (Join-Path $PSScriptRoot 'up.ps1')

# 2. Build
Write-Host "  [2/3] Build" -ForegroundColor White
if ($SkipBuild) {
    Assert-Jar
    Write-Host "  reusing $script:JarPath" -ForegroundColor DarkGray
} else {
    Invoke-Build
}

# 3. Launch
Write-Host ""
Write-Host "  [3/3] Launching consumer and producer" -ForegroundColor White

$consumerScript = Join-Path $PSScriptRoot 'run-consumer.ps1'
$producerScript = Join-Path $PSScriptRoot 'run-producer.ps1'

# -NoExit keeps each window open afterwards so the final totals stay readable.
Start-Process -FilePath 'powershell.exe' `
    -ArgumentList '-NoExit', '-ExecutionPolicy', 'Bypass', '-File', "`"$consumerScript`""

# The consumer needs a few seconds to join its group and be assigned partitions. Producing
# before then would still work -- the messages are durable -- but the dashboard would sit empty
# and then jump, which reads like a bug during a demo.
Write-Host "  waiting for the consumer to join its group..." -ForegroundColor DarkGray
Start-Sleep -Seconds 8

Start-Process -FilePath 'powershell.exe' `
    -ArgumentList '-NoExit', '-ExecutionPolicy', 'Bypass', '-File', "`"$producerScript`"", '-Rate', $Rate

Write-Host ""
Write-Host "  ============================================================" -ForegroundColor Cyan
Write-Host "   Two windows are now open: the consumer dashboard and the" -ForegroundColor Cyan
Write-Host "   producer log." -ForegroundColor Cyan
Write-Host ""
Write-Host "   Kafka UI     http://localhost:8080" -ForegroundColor Cyan
Write-Host "   Inspect DLQ  .\scripts\run-dlq-viewer.ps1" -ForegroundColor Cyan
Write-Host "   Stop all     .\scripts\down.ps1" -ForegroundColor Cyan
Write-Host "  ============================================================" -ForegroundColor Cyan
Write-Host ""
