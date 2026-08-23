<#
.SYNOPSIS
    Consumes order messages, aggregates prices in real time, retries transient failures and
    dead-letters the rest.
.PARAMETER FailureRate
    Probability that a valid order hits a simulated downstream outage. Set to 1.0 to force
    every record through its full retry budget and into the DLQ.
.PARAMETER MaxAttempts
    Total attempts per record, including the first.
.PARAMETER Group
    Consumer group id. Use a fresh name to re-read the topic from the beginning.
.PARAMETER NoDashboard
    Print plain log lines instead of the live panel. Use this if the terminal does not render
    ANSI escape sequences.
.EXAMPLE
    .\scripts\run-consumer.ps1
.EXAMPLE
    # Force every message to exhaust its retries, to demonstrate the DLQ filling up.
    .\scripts\run-consumer.ps1 -FailureRate 1.0
.EXAMPLE
    # Re-read the whole topic from offset zero under a new group.
    .\scripts\run-consumer.ps1 -Group replay-1
#>
[CmdletBinding()]
param(
    [double]$FailureRate,
    [int]$MaxAttempts,
    [string]$Group,
    [switch]$NoDashboard
)

. (Join-Path $PSScriptRoot '_common.ps1')

if ($PSBoundParameters.ContainsKey('FailureRate')) { $env:TRANSIENT_FAILURE_RATE = $FailureRate }
if ($PSBoundParameters.ContainsKey('MaxAttempts')) { $env:MAX_ATTEMPTS = $MaxAttempts }
if ($PSBoundParameters.ContainsKey('Group'))       { $env:CONSUMER_GROUP = $Group }
if ($NoDashboard)                                  { $env:DASHBOARD_ENABLED = 'false' }

Start-JavaMain -MainClass 'com.assignment.orders.consumer.OrderConsumer'
