@echo off
wsl.exe -d DataQualityRedis -u root -- service redis-server start
if errorlevel 1 exit /b 1
"D:\python313\python.exe" "%~dp0check_redis.py"
