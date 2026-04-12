@echo off
setlocal

echo.
echo  ==============================================
echo      DataPulse - Local Development Start
echo  ==============================================
echo.

cd /d "%~dp0"

set "VENV_PY=.venv\Scripts\python.exe"

echo [1/3] Checking Python virtual environment...
if not exist "%VENV_PY%" (
    echo   Creating venv...
    python -m venv .venv
)

if not exist "%VENV_PY%" (
    echo ERROR: Could not find %VENV_PY%
    exit /b 1
)

echo [2/3] Installing/checking Python dependencies...
"%VENV_PY%" -m pip install -r requirements.txt --quiet

echo [3/3] Starting FastAPI backend on http://localhost:8000
echo       (Press Ctrl+C to stop, then run: cd frontend and npm run dev)
echo.
echo IMPORTANT: Make sure .env file exists with your DB credentials!
echo.

"%VENV_PY%" -m uvicorn backend.api:app --reload --host 0.0.0.0 --port 8000
