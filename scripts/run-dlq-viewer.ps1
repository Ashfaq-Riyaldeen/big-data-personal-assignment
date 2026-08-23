<#
.SYNOPSIS
    Prints everything currently in the dead letter queue, with the diagnostics attached to each
    failed message.
.DESCRIPTION
    Reads orders.DLQ from the beginning without joining a consumer group, so it never consumes
    the queue or disturbs any other reader. Safe to run as often as you like.
.EXAMPLE
    .\scripts\run-dlq-viewer.ps1
#>
[CmdletBinding()]
param()

. (Join-Path $PSScriptRoot '_common.ps1')

Start-JavaMain -MainClass 'com.assignment.orders.dlq.DlqViewer'
