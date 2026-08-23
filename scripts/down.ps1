<#
.SYNOPSIS
    Stops the Kafka stack.
.PARAMETER Purge
    Also deletes the broker's data volume, discarding every topic and message. Use this to
    start the demo from a genuinely clean slate.
#>
[CmdletBinding()]
param(
    [switch]$Purge
)

. (Join-Path $PSScriptRoot '_common.ps1')

Write-Host ""
if ($Purge) {
    Write-Host "  Stopping the stack and deleting all topic data" -ForegroundColor Yellow
    docker compose -f $script:ComposeFile down -v
} else {
    Write-Host "  Stopping the stack (topic data is kept)" -ForegroundColor Cyan
    docker compose -f $script:ComposeFile down
}

Write-Host ""
