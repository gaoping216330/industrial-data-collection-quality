@echo off
cd /d "%~dp0"
wsl.exe -d DataQualityRedis -u root -- service redis-server start
if errorlevel 1 exit /b 1
"D:\python313\python.exe" "%~dp0cache_service.py"
pause
