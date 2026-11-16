param(
    [ValidateSet('start', 'stop', 'status')]
    [string]$Action = 'status'
)

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$PidFile = Join-Path $Root 'online_model_detector.pid'
$Python = 'C:\Users\LG TianChe\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe'
$Script = Join-Path $Root 'online_model_detector.py'
$Config = Join-Path $Root 'config.json'

function Get-ServiceProcess {
    if (-not (Test-Path -LiteralPath $PidFile)) { return $null }
    $servicePid = Get-Content -LiteralPath $PidFile -ErrorAction SilentlyContinue
    if (-not $servicePid) { return $null }
    return Get-Process -Id ([int]$servicePid) -ErrorAction SilentlyContinue
}

if ($Action -eq 'status') {
    $process = Get-ServiceProcess
    if ($null -eq $process) {
        Write-Output 'STOPPED'
        exit 1
    }
    Write-Output "RUNNING PID=$($process.Id) STARTED=$($process.StartTime)"
    exit 0
}

if ($Action -eq 'start') {
    $process = Get-ServiceProcess
    if ($null -ne $process) {
        Write-Output "ALREADY_RUNNING PID=$($process.Id)"
        exit 0
    }
    $env:PYTHONPATH = "$(Join-Path $Root '.deps');$(Resolve-Path (Join-Path $Root '..'));$env:PYTHONPATH"
    $process = Start-Process `
        -FilePath $Python `
        -ArgumentList @($Script, '--config', $Config) `
        -WorkingDirectory $Root `
        -WindowStyle Hidden `
        -PassThru
    Set-Content -LiteralPath $PidFile -Value $process.Id -Encoding ascii
    Start-Sleep -Seconds 3
    if ($null -eq (Get-Process -Id $process.Id -ErrorAction SilentlyContinue)) {
        Write-Output 'START_FAILED_CHECK_LOG'
        exit 2
    }
    Write-Output "STARTED PID=$($process.Id)"
    exit 0
}

$process = Get-ServiceProcess
if ($null -eq $process) {
    Write-Output 'ALREADY_STOPPED'
    exit 0
}
Stop-Process -Id $process.Id -ErrorAction Stop
Remove-Item -LiteralPath $PidFile -Force -ErrorAction SilentlyContinue
Write-Output "STOPPED PID=$($process.Id)"
