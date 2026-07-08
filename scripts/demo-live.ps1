param(
    [int]$DurationSeconds = 120,
    [int]$IntervalMs = 300
)

$ErrorActionPreference = "Stop"

function Get-FreeTcpPort {
    $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
    $listener.Start()
    $port = $listener.LocalEndpoint.Port
    $listener.Stop()
    return $port
}

function Wait-TcpReady {
    param(
        [int]$Port,
        [int]$TimeoutSeconds = 20
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $client = New-Object System.Net.Sockets.TcpClient
        try {
            $iar = $client.BeginConnect("127.0.0.1", $Port, $null, $null)
            if ($iar.AsyncWaitHandle.WaitOne(250) -and $client.Connected) {
                return $true
            }
        }
        catch {
        }
        finally {
            $client.Close()
        }
        Start-Sleep -Milliseconds 200
    }
    return $false
}

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

if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
    if (Test-Path "C:\Program Files\Go\bin\go.exe") {
        $env:Path = "C:\Program Files\Go\bin;" + $env:Path
    }
}
if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
    Write-Host "Go is not installed. Run scripts/setup-tools.ps1 first."
    exit 1
}

$root = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $root

$runId = Get-Date -Format "yyyyMMdd-HHmmss"
$logsDir = Join-Path $root ("logs\\live\\" + $runId)
New-Item -ItemType Directory -Path $logsDir -Force | Out-Null

$p1 = Get-FreeTcpPort
$p2 = Get-FreeTcpPort
$p3 = Get-FreeTcpPort

$nodes = @(
    @{ Id = 1; Port = $p1; Peers = "127.0.0.1:$p2,127.0.0.1:$p3"; Log = (Join-Path $logsDir "node1.log") },
    @{ Id = 2; Port = $p2; Peers = "127.0.0.1:$p1,127.0.0.1:$p3"; Log = (Join-Path $logsDir "node2.log") },
    @{ Id = 3; Port = $p3; Peers = "127.0.0.1:$p1,127.0.0.1:$p2"; Log = (Join-Path $logsDir "node3.log") }
)

$procs = @()
$nodeProcs = @()
foreach ($n in $nodes) {
    New-Item -ItemType File -Path $n.Log -Force | Out-Null
    $cmd = 'set "NODE_ID={0}" && set "PORT={1}" && set "PEERS={2}" && cd /d "{3}" && go run . 1> "{4}" 2>&1' -f $n.Id, $n.Port, $n.Peers, $root, $n.Log
    $p = Start-Process -FilePath "cmd.exe" -ArgumentList "/c", $cmd -PassThru
    $procs += $p
    $nodeProcs += $p
}

foreach ($n in $nodes) {
    if (-not (Wait-TcpReady -Port $n.Port -TimeoutSeconds 25)) {
        Write-Host "node failed to become ready on port $($n.Port)"
        foreach ($p in $procs) {
            Stop-ProcessGracefully -Id $p.Id
        }
        exit 1
    }
}

$addrCsv = "127.0.0.1:$p1,127.0.0.1:$p2,127.0.0.1:$p3"
$loadLog = Join-Path $logsDir "loadgen.log"
New-Item -ItemType File -Path $loadLog -Force | Out-Null
$loadCmd = 'cd /d "{0}" && go run ./cmd/loadgen -addrs {1} -interval {2}ms -duration {3}s 1> "{4}" 2>&1' -f $root, $addrCsv, $IntervalMs, $DurationSeconds, $loadLog
$loadProc = Start-Process -FilePath "cmd.exe" -ArgumentList "/c", $loadCmd -PassThru
$procs += $loadProc

$pidsPath = Join-Path $logsDir "pids.txt"
$pidRows = @()
foreach ($p in $procs) {
    $proc = Get-Process -Id $p.Id -ErrorAction SilentlyContinue
    if ($proc) {
        $pidRows += ("{0}|{1}" -f $proc.Id, $proc.StartTime.ToUniversalTime().Ticks)
    }
}
$pidRows | Set-Content -Path $pidsPath
$latestPidsPath = Join-Path $root "logs\live\latest-pids.txt"
$pidRows | Set-Content -Path $latestPidsPath

Write-Host "Live demo started."
Write-Host "Node ports: $p1, $p2, $p3"
Write-Host "Run logs folder: $logsDir"
Write-Host "Streaming logs below. Press Ctrl+C to stop all demo processes."

try {
    Get-Content -Path @(
        (Join-Path $logsDir "node1.log"),
        (Join-Path $logsDir "node2.log"),
        (Join-Path $logsDir "node3.log"),
        $loadLog
    ) -Wait
}
finally {
    Write-Host "Stopping demo processes..."
    foreach ($p in $procs) {
        Stop-ProcessGracefully -Id $p.Id
    }
}
