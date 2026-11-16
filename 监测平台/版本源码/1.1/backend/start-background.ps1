$ErrorActionPreference = 'Stop'
$root = $PSScriptRoot
$python = 'D:\python313\python.exe'
New-Item -ItemType Directory -Path (Join-Path $root 'logs') -Force | Out-Null
foreach ($name in @('quality_engine','web_app','model_worker','mqtt_duplicate_monitor')) {
    $runner = if ($name -eq 'model_worker') {
        Join-Path $root '.python312\python.exe'
    } else { $python }
    $scriptPath = Join-Path $root ($name + '.py')
    $pidPath = Join-Path $root ($name + '.pid')
    if (Test-Path -LiteralPath $pidPath) {
        $existingId = [int](Get-Content -LiteralPath $pidPath)
        $existing = Get-CimInstance Win32_Process -Filter "ProcessId=$existingId"
        if ($existing -and $existing.CommandLine.Contains($scriptPath)) {
            Write-Output "$name already running: $existingId"
            continue
        }
    }
    $process = Start-Process -FilePath $runner -ArgumentList ('"' + $scriptPath + '"') -WorkingDirectory $root -WindowStyle Hidden -RedirectStandardOutput (Join-Path $root ('logs\' + $name + '.out.log')) -RedirectStandardError (Join-Path $root ('logs\' + $name + '.err.log')) -PassThru
    $process.Id | Set-Content -LiteralPath $pidPath
    Write-Output "$name started: $($process.Id)"
}
