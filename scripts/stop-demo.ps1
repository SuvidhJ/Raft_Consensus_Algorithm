$ErrorActionPreference = "Stop"

$root = Resolve-Path (Join-Path $PSScriptRoot "..")
$pidsPath = Join-Path $root "logs\live\latest-pids.txt"

function Stop-ProcessGracefully {
    param(
        [int]$Id,
        [int]$GraceMilliseconds = 1800
    )

    if (-not (Get-Process -Id $Id -ErrorAction SilentlyContinue)) {
        return
    }

    try {
        Stop-Process -Id $Id -ErrorAction SilentlyContinue
    }
    catch {
    }

    $deadline = (Get-Date).AddMilliseconds($GraceMilliseconds)
    while ((Get-Date) -lt $deadline) {
        if (-not (Get-Process -Id $Id -ErrorAction SilentlyContinue)) {
            return
        }
        Start-Sleep -Milliseconds 100
    }

    if (Get-Process -Id $Id -ErrorAction SilentlyContinue) {
        Stop-Process -Id $Id -Force
    }
}

if (-not (Test-Path $pidsPath)) {
    Write-Host "No demo PID file found at $pidsPath"
    exit 0
}

$pidRows = Get-Content $pidsPath | Where-Object { $_ -match '^\d+(\|\d+)?$' }
foreach ($row in $pidRows) {
    $parts = $row -split '\|'
    $procId = [int]$parts[0]
    $expectedTicks = $null
    if ($parts.Count -ge 2) {
        [void][long]::TryParse($parts[1], [ref]$expectedTicks)
    }

    $proc = Get-Process -Id $procId -ErrorAction SilentlyContinue
    if (-not $proc) {
        continue
    }

    if ($expectedTicks -ne $null -and $proc.StartTime.ToUniversalTime().Ticks -ne $expectedTicks) {
        Write-Host "Skipping PID $procId (start time mismatch)"
        continue
    }

    Stop-ProcessGracefully -Id $procId
    Write-Host "Stopped PID $procId"
}
Write-Host "Demo processes cleanup complete."
