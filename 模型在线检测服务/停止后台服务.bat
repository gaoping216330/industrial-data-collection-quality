@echo off
setlocal
"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -ExecutionPolicy Bypass -File "%~dp0service_control.ps1" stop
set "RC=%ERRORLEVEL%"
pause
exit /b %RC%
