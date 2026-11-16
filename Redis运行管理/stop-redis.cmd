@echo off
wsl.exe -d DataQualityRedis -u root -- service redis-server stop
pause
