@echo off
setlocal
cd /d "%~dp0"

set "PYTHON_EXE=C:\Users\LG TianChe\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe"
if exist "%~dp0.venv\Scripts\python.exe" set "PYTHON_EXE=%~dp0.venv\Scripts\python.exe"

set "PYTHONPATH=%~dp0.deps;%~dp0..;%PYTHONPATH%"
"%PYTHON_EXE%" "%~dp0online_model_detector.py" --config "%~dp0config.json"

if errorlevel 1 pause
endlocal
