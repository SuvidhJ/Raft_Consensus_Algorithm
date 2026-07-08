$ErrorActionPreference = "Stop"

$localProtocBin = Resolve-Path (Join-Path $PSScriptRoot "..\tools\protoc\bin") -ErrorAction SilentlyContinue
if ($localProtocBin) {
    $env:Path = $localProtocBin.Path + ";" + $env:Path
}

$goBin = Join-Path $env:USERPROFILE "go\bin"
if (Test-Path $goBin) {
    $env:Path = $goBin + ";" + $env:Path
}

if (-not (Get-Command protoc -ErrorAction SilentlyContinue)) {
    Write-Host "protoc not found. Run scripts/setup-tools.ps1 first."
    exit 1
}
if (-not (Get-Command protoc-gen-go -ErrorAction SilentlyContinue)) {
    Write-Host "protoc-gen-go not found. Run scripts/setup-tools.ps1 first."
    exit 1
}
if (-not (Get-Command protoc-gen-go-grpc -ErrorAction SilentlyContinue)) {
    Write-Host "protoc-gen-go-grpc not found. Run scripts/setup-tools.ps1 first."
    exit 1
}

protoc --go_out=. --go_opt=module=github.com/SuvidhJ/Raft_Consensus_Algorithm --go-grpc_out=. --go-grpc_opt=module=github.com/SuvidhJ/Raft_Consensus_Algorithm proto/kv.proto proto/raft.proto
Write-Host "Generated protobuf files in ./proto"
