$ErrorActionPreference = 'Stop'
$receiverRoot = $PSScriptRoot
$pidPath = Join-Path $receiverRoot 'receiver.pid'
if (Test-Path -LiteralPath $pidPath) {
    $receiverProcessId = [int](Get-Content -LiteralPath $pidPath)
    $existing = Get-CimInstance Win32_Process -Filter "ProcessId=$receiverProcessId"
    if ($existing -and $existing.CommandLine.Contains((Join-Path $receiverRoot 'cache_service.py'))) {
        Write-Output "Receiver already running: $receiverProcessId"
        exit 0
    }
}
& "$env:WINDIR\System32\wsl.exe" -d DataQualityRedis -u root -- service redis-server start
if ($LASTEXITCODE -ne 0) { throw 'Redis startup failed' }
New-Item -ItemType Directory -Path (Join-Path $receiverRoot 'logs') -Force | Out-Null
$process = Start-Process -FilePath 'D:\python313\python.exe' -ArgumentList ('"' + (Join-Path $receiverRoot 'cache_service.py') + '"') -WorkingDirectory $receiverRoot -WindowStyle Hidden -RedirectStandardOutput (Join-Path $receiverRoot 'logs\stdout.log') -RedirectStandardError (Join-Path $receiverRoot 'logs\stderr.log') -PassThru
$process.Id | Set-Content -LiteralPath $pidPath
Write-Output "Receiver started: $($process.Id)"
