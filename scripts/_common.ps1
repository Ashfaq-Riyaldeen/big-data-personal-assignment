# Shared helpers for the run scripts. Dot-source this, do not run it directly.
#
# The main job here is locating a usable JDK. JAVA_HOME on a developer machine is very often
# stale -- left pointing at a JDK that has since been uninstalled -- and Maven fails with an
# unhelpful error when that happens. Rather than requiring every user to fix their environment
# before the project will build, these scripts find a working JDK themselves and set JAVA_HOME
# for their own session only. Nothing outside the script is modified.

$ErrorActionPreference = 'Stop'

$script:RepoRoot = Split-Path -Parent $PSScriptRoot
$script:JarPath = Join-Path $script:RepoRoot 'target\kafka-avro-orders.jar'
$script:ComposeFile = Join-Path $script:RepoRoot 'docker\docker-compose.yml'

function Resolve-JavaHome {
    <#
    .SYNOPSIS
        Returns the path of a JDK that actually exists, preferring the current JAVA_HOME.
    #>

    # 1. An existing JAVA_HOME, but only if it really contains a JDK.
    if ($env:JAVA_HOME -and (Test-Path (Join-Path $env:JAVA_HOME 'bin\java.exe'))) {
        return $env:JAVA_HOME
    }

    # 2. The usual installation roots, newest version first.
    $roots = @(
        'C:\Program Files\Java',
        'C:\Program Files\Eclipse Adoptium',
        'C:\Program Files\Microsoft\jdk',
        'C:\Program Files\Amazon Corretto'
    )
    foreach ($root in $roots) {
        if (-not (Test-Path $root)) { continue }
        $found = Get-ChildItem -Path $root -Directory -ErrorAction SilentlyContinue |
            Where-Object { Test-Path (Join-Path $_.FullName 'bin\javac.exe') } |
            Sort-Object Name -Descending |
            Select-Object -First 1
        if ($found) { return $found.FullName }
    }

    # 3. Whatever `java` resolves to on PATH, walking back up out of bin\.
    $javaOnPath = Get-Command java.exe -ErrorAction SilentlyContinue
    if ($javaOnPath) {
        $binDir = Split-Path -Parent $javaOnPath.Source
        $candidate = Split-Path -Parent $binDir
        if (Test-Path (Join-Path $candidate 'bin\java.exe')) { return $candidate }
    }

    throw "No JDK found. Install JDK 17 or newer, or set JAVA_HOME to a valid JDK directory."
}

function Initialize-Java {
    <#
    .SYNOPSIS
        Points JAVA_HOME at a valid JDK for this session and reports which one was chosen.
    #>
    $resolved = Resolve-JavaHome
    if ($env:JAVA_HOME -ne $resolved) {
        Write-Host "  using JDK: $resolved" -ForegroundColor DarkGray
        $env:JAVA_HOME = $resolved
    }
    return $resolved
}

function Invoke-Build {
    <#
    .SYNOPSIS
        Builds the uber-jar with the Maven Wrapper.
    #>
    param([switch]$SkipTests)

    Initialize-Java | Out-Null
    Push-Location $script:RepoRoot
    try {
        $mvnw = Join-Path $script:RepoRoot 'mvnw.cmd'
        if ($SkipTests) {
            & $mvnw -B clean package '-DskipTests'
        } else {
            & $mvnw -B clean package
        }
        if ($LASTEXITCODE -ne 0) { throw "Build failed with exit code $LASTEXITCODE." }
    } finally {
        Pop-Location
    }
}

function Assert-Jar {
    <#
    .SYNOPSIS
        Builds the jar if it is not already present, so the run scripts work from a fresh clone.
    #>
    if (-not (Test-Path $script:JarPath)) {
        Write-Host "  jar not found, building first..." -ForegroundColor Yellow
        Invoke-Build -SkipTests
    }
    if (-not (Test-Path $script:JarPath)) {
        throw "Build completed but $script:JarPath is still missing."
    }
}

function Assert-Docker {
    <#
    .SYNOPSIS
        Verifies the Docker daemon is reachable, with a useful message when it is not.
    #>
    docker info --format '{{.ServerVersion}}' 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Docker is not running. Start Docker Desktop, wait for it to report Running, then try again."
    }
}

function Start-JavaMain {
    <#
    .SYNOPSIS
        Runs one of the project's entry points against the uber-jar.
    #>
    param(
        [Parameter(Mandatory = $true)][string]$MainClass,
        [string[]]$JavaArgs = @()
    )

    Initialize-Java | Out-Null
    Assert-Jar

    # The dashboard draws with ANSI escape sequences; UTF-8 keeps the output clean if the
    # console was left on a legacy code page.
    try { [Console]::OutputEncoding = New-Object System.Text.UTF8Encoding $false } catch { }

    $java = Join-Path $env:JAVA_HOME 'bin\java.exe'
    & $java '-Dfile.encoding=UTF-8' '-Dstdout.encoding=UTF-8' '-cp' $script:JarPath $MainClass @JavaArgs
}
