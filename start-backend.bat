@echo off
setlocal EnableDelayedExpansion

echo.
echo  ================================================
echo       DataPulse ^| Backend Server
echo  ================================================
echo.

REM Always work from the folder this .bat lives in
cd /d "%~dp0"

REM ── 1. Check Python ──────────────────────────────────────────────────────────
where python >nul 2>&1
if errorlevel 1 (
    echo  [ERROR] Python not found on PATH.
    echo.
    echo          Install Python 3.11 or later from: https://www.python.org/downloads/
    echo          Make sure to check "Add Python to PATH" during installation.
    echo.
    pause
    exit /b 1
)

for /f "tokens=2 delims= " %%V in ('python --version 2^>^&1') do set PY_VER=%%V
echo  [OK]  Python %PY_VER% found.

REM ── 2. Check .env file ───────────────────────────────────────────────────────
set "SCRIPT_DIR=%~dp0"
if not exist ".env" (
    echo.
    echo  [ERROR] .env file is missing!
    echo.
    echo          Ask the project owner to send you the .env file,
    echo          then place it here:
    echo.
    echo          !SCRIPT_DIR!.env
    echo.
    pause
    exit /b 1
)
echo  [OK]  .env file found.

REM ── 3. Create virtual environment if needed ──────────────────────────────────
if not exist ".venv\Scripts\python.exe" (
    echo  [1/3] Creating virtual environment...
    python -m venv .venv
    if errorlevel 1 (
        echo  [ERROR] Failed to create virtual environment.
        pause
        exit /b 1
    )
    echo  [OK]  .venv created.
) else (
    echo  [1/3] Virtual environment found.
)

REM ── 4. Activate venv ─────────────────────────────────────────────────────────
call ".venv\Scripts\activate.bat"

REM ── 5. Install / update dependencies ─────────────────────────────────────────
echo  [2/3] Installing Python dependencies...
python -m pip install --upgrade pip --quiet
python -m pip install greenlet==3.1.1 --prefer-binary --quiet
python -m pip install -r backend\requirements.txt --prefer-binary --quiet
if errorlevel 1 (
    echo  [ERROR] Failed to install dependencies. Check your internet connection.
    pause
    exit /b 1
)
echo  [OK]  Dependencies ready.

REM ── 6. Start server ───────────────────────────────────────────────────────────
echo  [3/3] Starting backend server...
echo.
echo  ================================================
echo   API running at:  http://localhost:8000
echo   Press Ctrl+C to stop
echo  ================================================
echo.

python -m uvicorn backend.api:app --reload --host 0.0.0.0 --port 8000

if errorlevel 1 (
    echo.
    echo  [ERROR] Server failed to start. Check the error above.
    pause
    exit /b 1
)