<#
.SYNOPSIS
    Streams randomised Avro order messages to the orders topic.
.PARAMETER Rate
    Orders per second.
.PARAMETER Count
    How many messages to send; 0 streams until Ctrl+C.
.PARAMETER BadRate
    Fraction of orders that break a business rule, driving the permanent-failure path.
.PARAMETER PoisonRate
    Fraction of messages published as non-Avro bytes, driving the poison-pill path.
.EXAMPLE
    .\scripts\run-producer.ps1
.EXAMPLE
    .\scripts\run-producer.ps1 -Rate 25 -BadRate 0.2 -PoisonRate 0.1
.EXAMPLE
    # A clean stream with no injected faults, for showing the happy path on its own.
    .\scripts\run-producer.ps1 -BadRate 0 -PoisonRate 0
#>
[CmdletBinding()]
param(
    [double]$Rate,
    [long]$Count,
    [double]$BadRate,
    [double]$PoisonRate
)

. (Join-Path $PSScriptRoot '_common.ps1')

# Only override what was actually passed, so the application's own defaults still apply.
if ($PSBoundParameters.ContainsKey('Rate'))       { $env:PRODUCE_RATE_PER_SEC = $Rate }
if ($PSBoundParameters.ContainsKey('Count'))      { $env:TOTAL_MESSAGES = $Count }
if ($PSBoundParameters.ContainsKey('BadRate'))    { $env:BAD_RECORD_RATE = $BadRate }
if ($PSBoundParameters.ContainsKey('PoisonRate')) { $env:POISON_RATE = $PoisonRate }

Start-JavaMain -MainClass 'com.assignment.orders.producer.OrderProducer'
