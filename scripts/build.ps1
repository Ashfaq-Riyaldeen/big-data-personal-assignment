<#
.SYNOPSIS
    Builds the project with the Maven Wrapper.
.DESCRIPTION
    Generates Order.java from order.avsc, compiles, runs the unit tests, and shades everything
    into target/kafka-avro-orders.jar. No Maven installation is required -- the wrapper fetches
    the pinned distribution on first use.
.PARAMETER SkipTests
    Skip the unit tests. Handy when iterating, but the full build is what should be committed.
#>
[CmdletBinding()]
param(
    [switch]$SkipTests
)

. (Join-Path $PSScriptRoot '_common.ps1')

Write-Host ""
Write-Host "  Building kafka-avro-orders" -ForegroundColor Cyan
Write-Host "  ------------------------------------------------------------"

Invoke-Build -SkipTests:$SkipTests

Write-Host ""
Write-Host "  ------------------------------------------------------------"
Write-Host "  Built $script:JarPath" -ForegroundColor Green
Write-Host ""
