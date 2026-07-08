param(
    [switch]$SkipDocker
)

$ErrorActionPreference = "Stop"

function Test-Tool {
    param([string]$Name)
    return [bool](Get-Command $Name -ErrorAction SilentlyContinue)
}

function Ensure-Winget {
    if (-not (Test-Tool "winget")) {
        Write-Host "winget not found. Please install App Installer from Microsoft Store."
        exit 1
    }
}

function Install-WithWinget {
    param(
        [string]$Id,
        [string]$Label
    )
    Write-Host "Installing $Label ($Id)..."
    winget install --id $Id -e --accept-package-agreements --accept-source-agreements
}

function Install-ProtocLocal {
    param([string]$Version = "27.3")

    $toolsDir = Join-Path $PSScriptRoot "..\tools"
    $protocDir = Join-Path $toolsDir "protoc"
    $zipPath = Join-Path $toolsDir "protoc.zip"
    $url = "https://github.com/protocolbuffers/protobuf/releases/download/v$Version/protoc-$Version-win64.zip"

    New-Item -ItemType Directory -Force -Path $toolsDir | Out-Null
    New-Item -ItemType Directory -Force -Path $protocDir | Out-Null

    Write-Host "Downloading protoc v$Version..."
    Invoke-WebRequest -Uri $url -OutFile $zipPath

    Write-Host "Extracting protoc..."
    Expand-Archive -Path $zipPath -DestinationPath $protocDir -Force
    Remove-Item $zipPath -Force

    Write-Host "Local protoc installed at: $protocDir"
    Write-Host "Temporary shell path command:"
    Write-Host ('$env:Path += ";' + (Join-Path $protocDir "bin") + '"')
}

Write-Host "Checking required tools..."
$hasGo = Test-Tool "go"
$hasDocker = Test-Tool "docker"
$hasProtoc = Test-Tool "protoc"

if (-not $hasGo -or (-not $hasDocker -and -not $SkipDocker)) {
    Ensure-Winget
}

if (-not $hasGo) {
    Install-WithWinget -Id "GoLang.Go" -Label "Go"
} else {
    Write-Host "Go already installed."
}

if (-not $SkipDocker) {
    if (-not $hasDocker) {
        Install-WithWinget -Id "Docker.DockerDesktop" -Label "Docker Desktop"
    } else {
        Write-Host "Docker already installed."
    }
}

if (-not $hasProtoc) {
    Install-ProtocLocal
} else {
    Write-Host "protoc already installed."
}

if (-not (Get-Command go -ErrorAction SilentlyContinue) -and (Test-Path "C:\Program Files\Go\bin\go.exe")) {
    $env:Path = "C:\Program Files\Go\bin;" + $env:Path
}

if (Get-Command go -ErrorAction SilentlyContinue) {
    Write-Host "Installing protoc Go plugins..."
    go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.31.0
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0
    Write-Host "Installed protoc-gen-go and protoc-gen-go-grpc to $env:USERPROFILE\go\bin"
} else {
    Write-Host "Go not yet available in current shell; run plugin install after terminal restart."
}

Write-Host "Done. Restart your terminal before running go commands."
Write-Host "Then run: powershell -ExecutionPolicy Bypass -File .\scripts\gen-proto.ps1"