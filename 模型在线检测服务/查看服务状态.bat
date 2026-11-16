@echo off
setlocal
set "PYTHON_EXE=C:\Users\LG TianChe\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe"
set "PYTHONPATH=%~dp0.deps;%~dp0..;%PYTHONPATH%"
"%PYTHON_EXE%" "%~dp0show_service_status.py" --config "%~dp0config.json"
set "RC=%ERRORLEVEL%"
pause
exit /b %RC%
