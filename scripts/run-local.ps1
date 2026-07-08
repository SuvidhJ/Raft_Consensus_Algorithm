$ErrorActionPreference = "Stop"

if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
    Write-Host "Go is not installed. Run scripts/setup-tools.ps1 first."
    exit 1
}

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
        [int]$TimeoutSeconds = 15
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

$port = Get-FreeTcpPort
Write-Host "Starting single-node Raft KV server on :$port..."
$env:NODE_ID = "1"
$env:PORT = "$port"
$env:PEERS = ""

$server = Start-Process -FilePath "go" -ArgumentList "run ." -PassThru -NoNewWindow
if (-not (Wait-TcpReady -Port $port -TimeoutSeconds 20)) {
    Stop-ProcessGracefully -Id $server.Id
    Write-Host "server did not become ready on port $port"
    exit 1
}

Write-Host "Running smoke test client (SET + GET)..."
go run ./cmd/client -addr "localhost:$port" -key hello -value world

Write-Host "Stopping server..."
if (Get-Process -Id $server.Id -ErrorAction SilentlyContinue) {
    Stop-ProcessGracefully -Id $server.Id
}
Write-Host "Local smoke test complete."