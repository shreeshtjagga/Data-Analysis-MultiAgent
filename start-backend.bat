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

set "PY_CMD=python"
where py >nul 2>&1
if not errorlevel 1 (
    py -3.12 --version >nul 2>&1
    if not errorlevel 1 set "PY_CMD=py -3.12"
    if "%PY_CMD%"=="python" (
        py -3.13 --version >nul 2>&1
        if not errorlevel 1 set "PY_CMD=py -3.13"
    )
    if "%PY_CMD%"=="python" (
        py -3.11 --version >nul 2>&1
        if not errorlevel 1 set "PY_CMD=py -3.11"
    )
)

for /f "tokens=2 delims= " %%V in ('%PY_CMD% --version 2^>^&1') do set PY_VER=%%V
echo  [OK]  Python %PY_VER% found.

for /f "tokens=1,2 delims=." %%A in ("%PY_VER%") do (
    set PY_MAJOR=%%A
    set PY_MINOR=%%B
)

if "%PY_MAJOR%"=="3" (
    if %PY_MINOR% GEQ 14 (
        echo.
        echo  [ERROR] Python %PY_VER% is not supported by this backend yet.
        echo.
        echo          The startup script installs greenlet 3.1.1, and that package
        echo          does not currently build on Python 3.14.
        echo.
        echo          Please install Python 3.11, 3.12, or 3.13 and run this script again.
        echo          Recommended: Python 3.12
        echo.
        pause
        exit /b 1
    )
)

REM ── 2. Check .env file ───────────────────────────────────────────────────────
set "SCRIPT_DIR=%~dp0"
if not exist ".env" if not exist "..\.env" (
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
    %PY_CMD% -m venv .venv
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
