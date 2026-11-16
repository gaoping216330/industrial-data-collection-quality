$ErrorActionPreference = 'Stop'
$syncRoot = $PSScriptRoot
$pidPath = Join-Path $syncRoot 'sync.pid'
if (Test-Path -LiteralPath $pidPath) {
    $oldId = [int](Get-Content -LiteralPath $pidPath)
    $oldProcess = Get-CimInstance Win32_Process -Filter "ProcessId=$oldId"
    if ($oldProcess -and $oldProcess.CommandLine.Contains('sync_autosensor.py')) {
        Write-Output "Catalog sync already running: $oldId"
        exit 0
    }
}
$process = Start-Process -FilePath 'D:\python313\python.exe' -ArgumentList @(('"' + (Join-Path $syncRoot 'sync_autosensor.py') + '"'),'--apply','--loop') -WorkingDirectory $syncRoot -WindowStyle Hidden -RedirectStandardOutput (Join-Path $syncRoot 'stdout.log') -RedirectStandardError (Join-Path $syncRoot 'stderr.log') -PassThru
$process.Id | Set-Content -LiteralPath $pidPath
Write-Output "Catalog sync started: $($process.Id)"
