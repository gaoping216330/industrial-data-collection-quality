@echo off
setlocal
cd /d "%~dp0"

set "PYTHON_EXE=C:\Users\LG TianChe\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe"
if exist "%~dp0.venv\Scripts\python.exe" set "PYTHON_EXE=%~dp0.venv\Scripts\python.exe"

if not exist "%~dp0.deps" mkdir "%~dp0.deps"
"%PYTHON_EXE%" -m pip install --target "%~dp0.deps" -r "%~dp0requirements.txt"
pause
endlocal
