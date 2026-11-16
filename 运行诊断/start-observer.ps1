$ErrorActionPreference='Stop'
$root=$PSScriptRoot
$observerPath=Join-Path $root 'service_observer.py'
$existing=Get-CimInstance Win32_Process -Filter "Name='python.exe'" | Where-Object {$_.CommandLine -and $_.CommandLine.Contains($observerPath)}
if($existing){Write-Output 'Diagnostic observer already running';exit 0}
New-Item -ItemType Directory -Path (Join-Path $root 'logs') -Force | Out-Null
$process=Start-Process -FilePath 'D:\python313\python.exe' -ArgumentList ('"'+(Join-Path $root 'service_observer.py')+'"') -WorkingDirectory $root -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $root 'logs/observer.out.log') -RedirectStandardError (Join-Path $root 'logs/observer.err.log')
Write-Output "Diagnostic observer requested: $($process.Id)"
